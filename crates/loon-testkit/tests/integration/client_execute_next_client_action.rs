use super::common::support;

use loon_client::executor::{
    execute_next_client_action, ExecutedNextLocalOnlyCreate, NextClientAction,
    UploadLocalCreateExecution,
};
use loon_client::local_fs::{observe_subtree_path, NamespacePathIndex};
use loon_client::planner::{PlannedActionRecord, PlannerDecision};
use loon_client::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_server::objectstore::fs::LocalFsStore;
use loon_server::objectstore::keys::{namespace_head, namespace_lease};
use loon_server::objectstore::ObjectStore;
use loon_testkit::scenario::Scenario;
use loon_server::core::control_types::{ControlObjectKind, HeadStateEnvelope, LeaseStateEnvelope};
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse, HeadState, InodeId,
    InodeKind, LeaseState, NamespaceId, RevisionNo,
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
        ) => match result {
            UploadLocalCreateExecution::Completed(result) => {
                assert_eq!(result.upload_reused, upload_local_create.upload_reused);
                result.dispatched
            }
            UploadLocalCreateExecution::Progressed(progress) => {
                panic!("expected completed upload_local_create, got {progress:?}")
            }
        },
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
        |_namespace_id, _inode_id, _parent_inode_id, _display_name| {
            unreachable!("inode target path resolver should not run without work")
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
            let planned_action = PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(77),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: initial.planned_local_only_action.created_at_ms - 1_000,
            };
            tx.upsert_local_file(&placeholder_local_row(&planned_action))?;
            tx.upsert_planned_action(&planned_action)?;
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
            let planned_action = PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(77),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: initial.planned_local_only_action.created_at_ms,
            };
            tx.upsert_local_file(&placeholder_local_row(&planned_action))?;
            tx.upsert_planned_action(&planned_action)?;
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

#[test]
fn execute_next_client_action_prefers_bound_delete_file_for_same_path_replacement() {
    let temp_dir = TestDir::new("client-execute-next-client-action-replacement-file");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let namespace_id = demo_namespace();
    seed_head_and_lease(
        &store,
        &demo_head(namespace_id.clone()),
        &demo_lease(namespace_id.clone()),
    );

    let replacement_id = ClientFileId::from("tmp:demo:00000000000000000001");
    let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
    seed_bound_root_directory_for_namespace(&mut db, &namespace_id);
    seed_bound_file_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(2),
        "notes",
        InodeId(1),
        "sha256:notes-v1",
    );
    db.planner_transaction("seed-same-path-file-replacement", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            decision: PlannerDecision::DeleteFile.as_str().to_owned(),
            reason: "local_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_100_000,
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
            last_local_change_ms: 1_700_000_101_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            decision: PlannerDecision::WaitForExactPathVacate.as_str().to_owned(),
            reason: "exact_path_blocked_by_bound_occupant".to_owned(),
            created_at_ms: 1_700_000_102_000,
        })?;
        Ok(())
    })
    .expect("seed same-path file replacement state");

    assert_eq!(
        db.load_next_runnable_planned_local_only_action()
            .expect("load next runnable local-only action"),
        None
    );

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        &ExecuteNextClientActionAction {
            source_path_relative: None,
            uploaded_at_ms: 1_700_000_103_000,
            created_at_ms: 1_700_000_104_000,
            writer_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms: 1_700_000_105_000,
        },
    )
    .expect("one action should be scheduled");

    match executed {
        NextClientAction::ExecutedDispatchInodeMutation(executed) => {
            assert_eq!(executed.decision, PlannerDecision::DeleteFile);
        }
        other => panic!("expected bound delete_file to win same-path replacement, got {other:?}"),
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen client db");
    assert_eq!(
        db.load_planned_local_only_action(&replacement_id)
            .expect("load retained replacement planned action")
            .map(|row| row.decision),
        Some(PlannerDecision::CreateRemoteDir.as_str().to_owned())
    );
}

#[test]
fn execute_next_client_action_prefers_bound_delete_subtree_for_same_path_replacement() {
    let temp_dir = TestDir::new("client-execute-next-client-action-replacement-dir");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let namespace_id = demo_namespace();
    seed_head_and_lease(
        &store,
        &demo_head(namespace_id.clone()),
        &demo_lease(namespace_id.clone()),
    );

    let replacement_id = ClientFileId::from("tmp:demo:00000000000000000001");
    let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
    seed_bound_root_directory_for_namespace(&mut db, &namespace_id);
    seed_bound_directory_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(2),
        "notes",
        Some(InodeId(1)),
    );
    db.planner_transaction("seed-same-path-dir-replacement", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            decision: PlannerDecision::DeleteSubtree.as_str().to_owned(),
            reason: "local_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_local_only_file(&LocalOnlyFileStateRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(1)),
            display_name: "notes".to_owned(),
            content_digest: Some("sha256:replacement-v1".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_201_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            decision: PlannerDecision::WaitForExactPathVacate.as_str().to_owned(),
            reason: "exact_path_blocked_by_bound_occupant".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })?;
        Ok(())
    })
    .expect("seed same-path dir replacement state");

    assert_eq!(
        db.load_next_runnable_planned_local_only_action()
            .expect("load next runnable local-only action"),
        None
    );

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        &ExecuteNextClientActionAction {
            source_path_relative: None,
            uploaded_at_ms: 1_700_000_203_000,
            created_at_ms: 1_700_000_204_000,
            writer_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms: 1_700_000_205_000,
        },
    )
    .expect("one action should be scheduled");

    match executed {
        NextClientAction::ExecutedDispatchInodeMutation(executed) => {
            assert_eq!(executed.decision, PlannerDecision::DeleteSubtree);
        }
        other => {
            panic!("expected bound delete_subtree to win same-path replacement, got {other:?}")
        }
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen client db");
    assert_eq!(
        db.load_planned_local_only_action(&replacement_id)
            .expect("load retained replacement planned action")
            .map(|row| row.decision),
        Some(PlannerDecision::UploadLocalCreate.as_str().to_owned())
    );
}

#[test]
fn exact_path_waiting_local_only_create_wakes_after_bound_remote_rename() {
    let temp_dir = TestDir::new("client-execute-next-client-action-wake-after-rename");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let namespace_id = demo_namespace();
    seed_head_and_lease(
        &store,
        &demo_head(namespace_id.clone()),
        &demo_lease(namespace_id.clone()),
    );

    let replacement_id = ClientFileId::from("tmp:demo:00000000000000000009");
    let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
    seed_bound_root_directory_for_namespace(&mut db, &namespace_id);
    seed_bound_directory_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(3),
        "archive",
        Some(InodeId(1)),
    );
    seed_bound_file_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(2),
        "notes",
        InodeId(1),
        "sha256:notes-v1",
    );
    db.planner_transaction("seed-waiting-replacement-before-remote-rename", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(2),
            revision_no: RevisionNo(1),
            content_digest: Some("sha256:notes-v1".to_owned()),
            content_manifest_digest: Some("manifest:sha256:notes-v1".to_owned()),
            parent_inode_id: Some(InodeId(3)),
            display_name: "notes.txt".to_owned(),
            is_deleted: false,
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
            last_local_change_ms: 1_700_000_301_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            decision: PlannerDecision::WaitForExactPathVacate.as_str().to_owned(),
            reason: "exact_path_blocked_by_bound_occupant".to_owned(),
            created_at_ms: 1_700_000_302_000,
        })?;
        Ok(())
    })
    .expect("seed waiting replacement");

    db.apply_remote_rename(&namespace_id, InodeId(2), 1_700_000_303_000)
        .expect("apply remote rename should wake waiting replacement");

    assert_eq!(
        db.load_planned_local_only_action(&replacement_id)
            .expect("load replanned local-only action")
            .map(|row| row.decision),
        Some(PlannerDecision::CreateRemoteDir.as_str().to_owned())
    );

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        &ExecuteNextClientActionAction {
            source_path_relative: None,
            uploaded_at_ms: 1_700_000_304_000,
            created_at_ms: 1_700_000_305_000,
            writer_id: "writer-a".to_owned(),
            writer_version: "loon-server-test".to_owned(),
            now_ms: 1_700_000_306_000,
        },
    )
    .expect("woken local-only create should run");

    match executed {
        NextClientAction::ExecutedLocalOnlyCreate(ExecutedNextLocalOnlyCreate {
            planned_action,
            ..
        }) => {
            assert_eq!(planned_action.client_file_id, replacement_id);
            assert_eq!(
                planned_action.decision,
                PlannerDecision::CreateRemoteDir.as_str().to_owned()
            );
        }
        other => panic!("expected woken local-only create after rename, got {other:?}"),
    }
}

#[test]
fn execute_next_client_action_bound_file_to_dir_replacement_reaches_idle() {
    let temp_dir = TestDir::new("client-execute-next-client-action-file-to-dir-idle");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let namespace_id = demo_namespace();
    seed_head_and_lease(
        &store,
        &demo_head(namespace_id.clone()),
        &demo_lease(namespace_id.clone()),
    );

    let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
    seed_bound_root_directory_for_namespace(&mut db, &namespace_id);
    seed_bound_file_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(2),
        "notes",
        InodeId(1),
        "sha256:notes-v1",
    );

    fs::create_dir_all(mirror_root.join("notes")).expect("create replacement directory");
    observe_subtree_path(
        &db_path,
        &namespace_id,
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        1_700_000_300_000,
    )
    .expect("observe subtree replacement");

    let executed = run_execute_next_until_idle(
        &db_path,
        &store,
        &mirror_root,
        &namespace_id,
        "writer-a",
        "loon-server-test",
        1_700_000_301_000,
        8,
    );

    assert_eq!(executed.len(), 2, "expected delete then create directory");
    assert!(matches!(
        executed.first(),
        Some(NextClientAction::ExecutedDispatchInodeMutation(result))
            if result.decision == PlannerDecision::DeleteFile
    ));
    assert!(matches!(
        executed.get(1),
        Some(NextClientAction::ExecutedLocalOnlyCreate(_))
    ));

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB");
    let summary = db
        .load_namespace_state_summary(&namespace_id)
        .expect("load namespace summary");
    let parent_links = db
        .load_local_only_parent_links_for_namespace(&namespace_id)
        .expect("load parent links");
    let path_index = NamespacePathIndex::build(&summary, &parent_links);
    assert!(path_index.bound_file_matches("notes").is_empty());
    assert_eq!(path_index.bound_dir_matches("notes").len(), 1);
    assert!(summary.local_only_state.is_empty());
    assert!(summary.local_only_planned_actions.is_empty());
}

#[test]
fn execute_next_client_action_bound_dir_to_file_replacement_reaches_idle() {
    let temp_dir = TestDir::new("client-execute-next-client-action-dir-to-file-idle");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let mirror_root = temp_dir.path().join("mirror");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let namespace_id = demo_namespace();
    seed_head_and_lease(
        &store,
        &demo_head(namespace_id.clone()),
        &demo_lease(namespace_id.clone()),
    );

    let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
    seed_bound_root_directory_for_namespace(&mut db, &namespace_id);
    seed_bound_directory_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(2),
        "notes",
        Some(InodeId(1)),
    );
    seed_bound_file_for_namespace(
        &mut db,
        &namespace_id,
        InodeId(3),
        "todo.txt",
        InodeId(2),
        "sha256:todo-v1",
    );

    fs::write(mirror_root.join("notes"), b"replacement file\n").expect("write replacement file");
    observe_subtree_path(
        &db_path,
        &namespace_id,
        &mirror_root,
        temp_dir.path(),
        &mirror_root,
        1_700_000_400_000,
    )
    .expect("observe subtree replacement");

    let executed = run_execute_next_until_idle(
        &db_path,
        &store,
        &mirror_root,
        &namespace_id,
        "writer-a",
        "loon-server-test",
        1_700_000_401_000,
        8,
    );

    assert_eq!(
        executed.len(),
        2,
        "expected delete subtree then create file"
    );
    assert!(matches!(
        executed.first(),
        Some(NextClientAction::ExecutedDispatchInodeMutation(result))
            if result.decision == PlannerDecision::DeleteSubtree
    ));
    assert!(matches!(
        executed.get(1),
        Some(NextClientAction::ExecutedLocalOnlyCreate(_))
    ));

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB");
    let summary = db
        .load_namespace_state_summary(&namespace_id)
        .expect("load namespace summary");
    let parent_links = db
        .load_local_only_parent_links_for_namespace(&namespace_id)
        .expect("load parent links");
    let path_index = NamespacePathIndex::build(&summary, &parent_links);
    assert!(path_index.bound_dir_matches("notes").is_empty());
    assert_eq!(path_index.bound_file_matches("notes").len(), 1);
    assert!(path_index.bound_file_matches("notes/todo.txt").is_empty());
    assert!(summary.local_only_state.is_empty());
    assert!(summary.local_only_planned_actions.is_empty());
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
        |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
        action.uploaded_at_ms,
        action.created_at_ms,
        |request| {
            support::seed_server_basis_for_request(store, request, &action.writer_version);
            execute_client_mutation(
                store,
                request,
                &ClientMutationExecutionParams {
                    writer_id: action.writer_id.clone(),
                    writer_version: action.writer_version.clone(),
                    now_ms: action.now_ms,
                    lease_duration_ms: 60_000,
                },
            )
            .map(|executed| executed.response)
            .map_err(|err| err.to_string())
        },
    )
    .expect("execute next client action")
}

#[allow(clippy::too_many_arguments)]
fn run_execute_next_until_idle(
    db_path: &Path,
    store: &LocalFsStore,
    mirror_root: &Path,
    namespace_id: &NamespaceId,
    writer_id: &str,
    writer_version: &str,
    start_now_ms: u64,
    max_steps: usize,
) -> Vec<NextClientAction> {
    let mut executed = Vec::new();
    for step in 0..max_steps {
        let step_ms = start_now_ms + (step as u64 * 1_000);
        let mut db = SqliteStateDb::open(db_path).expect("open client state DB for loop step");
        let summary = db
            .load_namespace_state_summary(namespace_id)
            .expect("load namespace summary for loop step");
        let parent_links = db
            .load_local_only_parent_links_for_namespace(namespace_id)
            .expect("load parent links for loop step");
        let path_index = NamespacePathIndex::build(&summary, &parent_links);
        let local_only_paths = path_index.clone();
        let current_paths = path_index.clone();
        let target_paths = path_index;

        let next = execute_next_client_action(
            &mut db,
            store,
            |client_file_id| {
                local_only_paths
                    .resolve_local_only_source_relative_path(client_file_id)
                    .map(|relative_path| mirror_root.join(relative_path))
            },
            |_namespace_id, inode_id| {
                current_paths
                    .resolve_current_inode_relative_path(inode_id)
                    .map(|relative_path| mirror_root.join(relative_path))
            },
            |_namespace_id, inode_id, parent_inode_id, display_name| {
                target_paths
                    .resolve_target_inode_relative_path(inode_id, parent_inode_id, display_name)
                    .map(|relative_path| mirror_root.join(relative_path))
            },
            step_ms,
            step_ms + 1,
            |request| {
                support::seed_server_basis_for_request(store, request, writer_version);
                execute_client_mutation(
                    store,
                    request,
                    &ClientMutationExecutionParams {
                        writer_id: writer_id.to_owned(),
                        writer_version: writer_version.to_owned(),
                        now_ms: step_ms + 2,
                        lease_duration_ms: 60_000,
                    },
                )
                .map(|executed| executed.response)
                .map_err(|err| err.to_string())
            },
        )
        .expect("execute next client action in loop");

        match next {
            Some(next) => executed.push(next),
            None => return executed,
        }
    }

    panic!("client did not reach idle within {max_steps} steps");
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
        tx.upsert_local_file(&placeholder_local_row(planned_action))?;
        tx.upsert_planned_action(planned_action)?;
        Ok(())
    })
    .expect("seed planned action state");
}

fn placeholder_local_row(planned_action: &PlannedActionRow) -> LocalFileStateRow {
    let inode_kind = if planned_action.decision == "materialize_remote_dir" {
        InodeKind::Dir
    } else {
        InodeKind::File
    };
    LocalFileStateRow {
        namespace_id: planned_action.namespace_id.clone(),
        inode_id: planned_action.inode_id,
        inode_kind: inode_kind.clone(),
        content_digest: if inode_kind == InodeKind::Dir {
            None
        } else {
            Some(format!(
                "sha256:planned-{}-{}",
                planned_action.namespace_id.as_str(),
                planned_action.inode_id.0
            ))
        },
        parent_inode_id: Some(InodeId(2)),
        display_name: format!("inode-{}", planned_action.inode_id.0),
        exists_on_disk: inode_kind != InodeKind::Dir,
        dirty: false,
        last_local_change_ms: planned_action.created_at_ms,
    }
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

fn seed_bound_root_directory_for_namespace(db: &mut SqliteStateDb, namespace_id: &NamespaceId) {
    db.planner_transaction("seed-bound-root-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
            is_deleted: false,
        })?;
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
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
        })?;
        Ok(())
    })
    .expect("seed bound root directory");
}

fn seed_bound_file_for_namespace(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: InodeId,
    content_digest: &str,
) {
    db.planner_transaction("seed-bound-file", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(format!("manifest:{content_digest}")),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            content_digest: Some(content_digest.to_owned()),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: Some(content_digest.to_owned()),
            content_manifest_digest: Some(format!("manifest:{content_digest}")),
            parent_inode_id: Some(parent_inode_id),
            display_name: display_name.to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound file");
}

fn seed_bound_directory_for_namespace(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: Option<InodeId>,
) {
    db.planner_transaction("seed-bound-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(1),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id,
            display_name: display_name.to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound directory");
}

fn demo_namespace() -> NamespaceId {
    NamespaceId::from("demo")
}

fn demo_head(namespace_id: NamespaceId) -> HeadState {
    HeadState {
        namespace_id,
        seq: ChangeSeq(41),
        active_fence_token: loon_types::FenceToken(8),
        next_inode_id: InodeId(501),
        snapshot_hint_seq: Some(ChangeSeq(40)),
        retention_floor_seq: ChangeSeq(40),
    }
}

fn demo_lease(namespace_id: NamespaceId) -> LeaseState {
    LeaseState {
        namespace_id,
        holder_id: "writer-a".to_owned(),
        fence_token: loon_types::FenceToken(8),
        lease_expires_at_ms: 1_700_000_600_000,
    }
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
