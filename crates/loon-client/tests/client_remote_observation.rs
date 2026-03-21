use loon_client::executor::{
    execute_next_client_action, ExecuteApplyRemoteDeleteError, ExecuteApplyRemoteRenameError,
    ExecuteApplyRemoteSubtreeDeleteError, ExecuteApplyRemoteSubtreeRenameError,
    ExecuteNextClientActionError, ExecutedApplyRemoteDelete, ExecutedApplyRemoteRename,
    ExecutedApplyRemoteSubtreeDelete, ExecutedApplyRemoteSubtreeRename, NextClientAction,
};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, BoundLocalOnlyFile, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, ObservedRemoteInode, PendingClientMutationRow,
    PendingInodeMutationRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
    TransferDirection, TransferLedgerRow, TransferState,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::content_manifest;
use loon_testkit::invariants::{
    evaluate_apply_remote_delete_invariants, evaluate_apply_remote_rename_invariants,
    evaluate_apply_remote_subtree_delete_invariants,
    evaluate_apply_remote_subtree_rename_invariants, evaluate_remote_delete_planning_invariants,
    evaluate_remote_observation_active_download_invariants,
    evaluate_remote_observation_active_upload_invariants,
    evaluate_remote_observation_ambiguous_bind_invariants,
    evaluate_remote_observation_convergence_invariants,
    evaluate_remote_observation_late_bind_invariants,
    evaluate_remote_path_change_planning_invariants,
    evaluate_remote_subtree_delete_planning_invariants,
    evaluate_remote_subtree_rename_planning_invariants, ApplyRemoteDeleteInvariantInputs,
    ApplyRemoteRenameInvariantInputs, ApplyRemoteSubtreeDeleteInvariantInputs,
    ApplyRemoteSubtreeRenameInvariantInputs, ClientReconciliationInvariantReport,
    RemoteDeletePlanningInvariantInputs, RemoteObservationActiveDownloadInvariantInputs,
    RemoteObservationActiveUploadInvariantInputs, RemoteObservationAmbiguousBindInvariantInputs,
    RemoteObservationConvergenceInvariantInputs, RemoteObservationLateBindInvariantInputs,
    RemotePathChangePlanningInvariantInputs, RemoteSubtreeDeletePlanningInvariantInputs,
    RemoteSubtreeRenamePlanningInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{
    ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeId, InodeKind,
    NamespaceId, RevisionNo,
};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn remote_observation_binds_local_only_create_and_restarts_converged() {
    let scenario = load_fixture("client/client_remote_observation_binds_local_only_create.yaml");
    let initial: CreateObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: CreateObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-create");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let expected_pending_client_mutation = initial.pending_client_mutation.clone().into_row();

    let source_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    seed_create_observation_state(&db_path, &initial, &store, &source_path);

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
        db.load_local_only_upload(&initial.local_only_state.client_file_id)
            .expect("load temp local-only upload"),
        if expect.local_only_upload_cleared {
            None
        } else {
            panic!("fixture currently expects local-only upload to clear");
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load temp local-only action"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&initial.pending_client_mutation.client_request_id)
            .expect("load pending client mutation"),
        if expect.pending_client_mutation_cleared {
            None
        } else {
            Some(expected_pending_client_mutation)
        }
    );

    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load converged views for invariants");
    let report = evaluate_remote_observation_late_bind_invariants(
        RemoteObservationLateBindInvariantInputs {
            remote_present_after: views.remote.is_some(),
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            local_dirty_after: views.local.as_ref().is_some_and(|local| local.dirty),
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|remote| remote.content_digest.as_deref()),
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|local| local.content_digest.as_deref()),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|anchor| anchor.content_digest.as_deref()),
            local_only_file_present_after: db
                .load_local_only_file(&initial.local_only_state.client_file_id)
                .expect("load local-only file after bind")
                .is_some(),
            planned_local_only_action_present_after: db
                .load_planned_local_only_action(&initial.local_only_state.client_file_id)
                .expect("load local-only plan after bind")
                .is_some(),
            local_only_upload_present_after: db
                .load_local_only_upload(&initial.local_only_state.client_file_id)
                .expect("load local-only upload after bind")
                .is_some(),
            local_only_transfer_present_after: db
                .load_local_only_transfer_ledger(
                    &initial.local_only_state.client_file_id,
                    TransferDirection::Upload,
                )
                .expect("load local-only transfer ledger after bind")
                .is_some(),
            local_only_issue_count_after: db
                .load_local_only_conflicts_and_errors(&initial.local_only_state.client_file_id)
                .expect("load local-only issues after bind")
                .len(),
            pending_client_mutation_present_after: db
                .load_pending_client_mutation(&initial.pending_client_mutation.client_request_id)
                .expect("load pending client mutation after bind")
                .is_some(),
        },
    );
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_converges_bound_file_edit_and_restarts_clean() {
    let scenario = load_fixture("client/client_remote_observation_converges_bound_file_edit.yaml");
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-edit");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let expected_pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);

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
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action"),
        if expect.planned_action_cleared {
            None
        } else {
            Some(initial.planned_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
            .expect("load pending inode mutation"),
        if expect.pending_inode_mutation_cleared {
            None
        } else {
            Some(expected_pending_inode_mutation)
        }
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after observation"),
        initial.local_file.content_utf8
    );

    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load converged views for invariants");
    let report = evaluate_remote_observation_convergence_invariants(
        RemoteObservationConvergenceInvariantInputs {
            planned_action_present_after: db
                .load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
                .expect("load planned action after convergence")
                .is_some(),
            pending_inode_mutation_present_after: db
                .load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
                .expect("load pending inode mutation after convergence")
                .is_some(),
            local_dirty_after: views.local.as_ref().is_some_and(|local| local.dirty),
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|local| local.content_digest.as_deref()),
            remote_synced_seq_after: views
                .remote
                .as_ref()
                .expect("converged remote after observation")
                .observed_seq,
            remote_revision_no_after: views
                .remote
                .as_ref()
                .expect("converged remote after observation")
                .revision_no,
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|remote| remote.content_digest.as_deref()),
            sync_anchor_seq_after: views.sync_anchor.as_ref().map(|anchor| anchor.synced_seq),
            sync_anchor_revision_no_after: views
                .sync_anchor
                .as_ref()
                .map(|anchor| anchor.revision_no),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|anchor| anchor.content_digest.as_deref()),
        },
    );
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_ambiguous_bind_records_issue_and_preserves_local_only_state() {
    let scenario =
        load_fixture("client/client_remote_observation_ambiguous_bind_records_issue.yaml");
    let initial: AmbiguousObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: AmbiguousObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-ambiguous");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_ambiguous_observation_state(&db_path, &initial.local_only_state);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
            .expect("load ambiguous views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(601),
            remote: expect.remote_state.clone(),
            local: expect.local_state.clone(),
            sync_anchor: expect.sync_anchor.clone(),
        }
    );
    let actual_issues = db
        .load_conflicts_and_errors(&NamespaceId::from("ns-1"), InodeId(601))
        .expect("load durable issues")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(actual_issues, expect.conflicts_and_errors);
    let surviving_local_only = initial
        .local_only_state
        .iter()
        .filter(|row| {
            db.load_local_only_file(&row.client_file_id)
                .expect("load local-only row")
                .is_some()
        })
        .count();
    assert_eq!(surviving_local_only, expect.local_only_state_count);

    let views = db
        .load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
        .expect("load views after ambiguous observation");
    let report = evaluate_remote_observation_ambiguous_bind_invariants(
        RemoteObservationAmbiguousBindInvariantInputs {
            issue_kind_after: actual_issues.first().map(|issue| issue.kind.as_str()),
            issue_matches_after: actual_issues
                .first()
                .and_then(|issue| issue.detail_json["matches"].as_u64())
                .map(|matches| matches as usize),
            remote_present_after: views.remote.is_some(),
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            surviving_local_only_count_after: surviving_local_only,
            initial_local_only_count: initial.local_only_state.len(),
        },
    );
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_updates_bound_file_while_upload_transfer_active() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_bound_file_while_upload_transfer_active.yaml",
    );
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-upload");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let expected_pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);

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
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load updated views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
            .expect("load pending inode mutation"),
        Some(expected_pending_inode_mutation)
    );
    assert!(
        db.load_transfer_ledger_for_inode(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            TransferDirection::Upload,
        )
        .expect("load upload transfer ledger")
        .is_some(),
        "active upload transfer should survive late observation",
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after observation"),
        initial.local_file.content_utf8
    );
}

#[test]
fn remote_observation_updates_remote_only_file_while_download_transfer_active() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_remote_only_file_while_download_transfer_active.yaml",
    );
    let initial: DownloadObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DownloadObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-download");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_remote_only_observation_state(&db_path, &initial);

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
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load remote-only views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: None,
        }
    );
    let actual_planned_action = db
        .load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load planned action after late download observation");
    if expect.planned_action_cleared {
        assert_eq!(actual_planned_action, None);
    } else {
        let actual_planned_action =
            actual_planned_action.expect("planned action should remain after observation");
        assert_eq!(
            actual_planned_action.decision,
            initial.planned_action.decision
        );
        assert_eq!(actual_planned_action.reason, initial.planned_action.reason);
        assert_eq!(
            actual_planned_action.namespace_id,
            initial.planned_action.namespace_id
        );
        assert_eq!(
            actual_planned_action.inode_id,
            initial.planned_action.inode_id
        );
    }
    assert!(
        db.load_transfer_ledger_for_inode(
            &planner_tick.namespace_id,
            planner_tick.inode_id,
            TransferDirection::Download,
        )
        .expect("load download transfer ledger")
        .is_some(),
        "active download transfer should survive late observation",
    );
}

#[test]
fn remote_observation_bound_file_remote_rename_applies_locally() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_rename_applies_locally.yaml",
        false,
    );

    assert!(
        report.final_target_exists,
        "renamed target path should exist"
    );
    assert!(
        !report.final_current_exists,
        "old local path should be removed after rename"
    );
}

#[test]
fn remote_observation_bound_file_remote_move_applies_locally() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_move_applies_locally.yaml",
        false,
    );

    assert!(report.final_target_exists, "moved target path should exist");
    assert!(
        !report.final_current_exists,
        "old local path should be removed after move"
    );
}

#[test]
fn remote_observation_bound_file_remote_rename_destination_occupied_records_issue_and_retries() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_rename_destination_occupied_records_issue.yaml",
        true,
    );

    assert!(
        report.final_target_exists,
        "target path should exist after retry"
    );
    assert!(
        !report.final_current_exists,
        "current path should be removed after retry"
    );
}

#[test]
fn remote_observation_bound_file_remote_delete_applies_locally() {
    let report = run_remote_delete_invariant_report(
        "client/client_remote_observation_bound_file_remote_delete_applies_locally.yaml",
    );

    assert!(
        !report.final_current_exists,
        "deleted local path should be removed after remote delete apply"
    );
}

#[test]
fn remote_observation_bound_file_remote_delete_missing_current_path_records_issue() {
    let report = run_remote_delete_invariant_report(
        "client/client_remote_observation_bound_file_remote_delete_current_path_missing_records_issue.yaml",
    );

    assert!(
        !report.final_current_exists,
        "missing current path fixture should leave the file absent"
    );
}

#[test]
fn remote_observation_bound_directory_remote_subtree_delete_applies_locally() {
    let report = run_remote_subtree_delete_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_delete_applies_locally.yaml",
    );

    assert!(
        !report.final_root_exists,
        "deleted local subtree root should be removed after remote subtree delete apply"
    );
}

#[test]
fn remote_observation_bound_directory_remote_subtree_delete_missing_current_path_records_issue() {
    let report = run_remote_subtree_delete_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_delete_current_path_missing_records_issue.yaml",
    );

    assert!(
        !report.final_root_exists,
        "missing root path fixture should leave the subtree absent"
    );
}

#[test]
fn remote_observation_remote_subtree_delete_while_descendant_differs_defers_conflict_copy() {
    let report = run_remote_subtree_delete_conflict_report(
        "client/client_remote_observation_deletes_bound_directory_while_descendant_differs_from_anchor.yaml",
    );

    assert!(
        report.final_root_exists,
        "remote subtree delete should not eagerly remove a locally diverged subtree"
    );
}

#[test]
fn remote_observation_remote_subtree_delete_while_descendant_busy_defers_conflict_copy() {
    let report = run_remote_subtree_delete_conflict_report(
        "client/client_remote_observation_deletes_bound_directory_while_descendant_upload_active.yaml",
    );

    assert!(
        report.final_root_exists,
        "remote subtree delete should not eagerly remove a busy subtree"
    );
}

#[test]
fn remote_observation_bound_directory_remote_subtree_rename_applies_locally() {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_rename_applies_locally.yaml",
        false,
    );

    assert!(report.final_target_exists, "renamed root path should exist");
    assert!(
        !report.final_root_exists,
        "old subtree root path should be removed after rename"
    );
}

#[test]
fn remote_observation_bound_directory_remote_subtree_move_applies_locally() {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_move_applies_locally.yaml",
        false,
    );

    assert!(
        report.final_target_exists,
        "moved subtree target path should exist"
    );
    assert!(
        !report.final_root_exists,
        "old subtree root path should be removed after move"
    );
}

#[test]
fn remote_observation_bound_directory_remote_subtree_rename_destination_occupied_records_issue_and_retries(
) {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_rename_destination_occupied_records_issue.yaml",
        true,
    );

    assert!(
        report.final_target_exists,
        "target subtree path should exist after retry"
    );
    assert!(
        !report.final_root_exists,
        "old subtree root path should be removed after retry"
    );
}

#[test]
fn remote_observation_remote_subtree_rename_into_unusable_parent_defers_conflict_copy() {
    let report = run_remote_subtree_rename_conflict_report(
        "client/client_remote_observation_renames_bound_directory_into_unusable_parent_defers_conflict_copy.yaml",
    );

    assert!(
        report.final_root_exists,
        "remote subtree rename should not eagerly move into an unusable parent"
    );
}

#[test]
fn remote_observation_remote_subtree_rename_while_descendant_differs_defers_conflict_copy() {
    let report = run_remote_subtree_rename_conflict_report(
        "client/client_remote_observation_renames_bound_directory_while_descendant_differs_from_anchor.yaml",
    );

    assert!(
        report.final_root_exists,
        "remote subtree rename should not eagerly move a subtree with diverged descendants"
    );
}

#[test]
fn remote_observation_remote_subtree_rename_while_descendant_busy_defers_conflict_copy() {
    let report = run_remote_subtree_rename_conflict_report(
        "client/client_remote_observation_renames_bound_directory_while_descendant_upload_active.yaml",
    );

    assert!(
        report.final_root_exists,
        "remote subtree rename should not eagerly move a busy subtree"
    );
}

#[test]
fn remote_observation_remote_delete_while_local_differs_from_anchor_defers_conflict_copy() {
    let scenario = load_fixture(
        "client/client_remote_observation_deletes_bound_file_while_local_differs_from_anchor.yaml",
    );
    let initial: DeleteObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DeleteConflictObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-delete-local-differs");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_remote_delete_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert!(
        local_path.exists(),
        "remote delete should not eagerly unlink a locally diverged file"
    );
}

#[test]
fn remote_observation_path_change_while_upload_transfer_active_does_not_move_eagerly() {
    let scenario = load_fixture(
        "client/client_remote_observation_renames_bound_file_while_upload_transfer_active.yaml",
    );
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-upload-rename");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    let renamed_path = source_root.join("reports/report-renamed.txt");

    seed_edit_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert!(
        local_path.exists(),
        "current path should remain during active upload"
    );
    assert!(
        !renamed_path.exists(),
        "late rename observation should not move the file eagerly"
    );
}

#[test]
fn remote_observation_delete_while_upload_transfer_active_does_not_unlink_eagerly() {
    let scenario = load_fixture(
        "client/client_remote_observation_deletes_bound_file_while_upload_transfer_active.yaml",
    );
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-upload-delete");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert!(
        local_path.exists(),
        "current path should remain during active upload"
    );

    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load bound views after delete observation");
    let report = evaluate_remote_observation_active_upload_invariants(
        RemoteObservationActiveUploadInvariantInputs {
            transfer_present_after: db
                .load_transfer_ledger_for_inode(
                    &planner_tick.namespace_id,
                    planner_tick.inode_id,
                    TransferDirection::Upload,
                )
                .expect("load upload transfer ledger")
                .is_some(),
            pending_inode_mutation_present_after: db
                .load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
                .expect("load pending inode mutation")
                .is_some(),
            remote_synced_seq_after: views
                .remote
                .as_ref()
                .expect("remote row after delete observation")
                .observed_seq,
            expected_remote_synced_seq: expect.remote_state.observed_seq,
        },
    );
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_path_change_while_download_transfer_active_does_not_move_eagerly() {
    let scenario = load_fixture(
        "client/client_remote_observation_renames_remote_only_file_while_download_transfer_active.yaml",
    );
    let initial: DownloadObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DownloadObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-download-rename");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(source_root.join("reports")).expect("create report dir");
    let placeholder_path = source_root.join("reports/report.txt");
    fs::write(&placeholder_path, "").expect("seed placeholder path");
    let renamed_path = source_root.join("reports/report-renamed.txt");

    seed_remote_only_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert!(
        !renamed_path.exists(),
        "late rename observation should not materialize the new path eagerly"
    );
}

#[test]
fn remote_observation_delete_while_download_transfer_active_does_not_unlink_eagerly() {
    let scenario = load_fixture(
        "client/client_remote_observation_deletes_bound_file_while_download_transfer_active.yaml",
    );
    let initial: BoundDownloadObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: BoundDownloadObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-download-delete");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_bound_download_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert!(
        local_path.exists(),
        "current path should remain during active download"
    );

    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load bound views after delete observation");
    assert_eq!(
        views,
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action after delete observation")
            .is_none(),
        expect.planned_action_cleared
    );
    let report = evaluate_remote_observation_active_download_invariants(
        RemoteObservationActiveDownloadInvariantInputs {
            transfer_present_after: db
                .load_transfer_ledger_for_inode(
                    &planner_tick.namespace_id,
                    planner_tick.inode_id,
                    TransferDirection::Download,
                )
                .expect("load download transfer ledger")
                .is_some(),
            remote_synced_seq_after: views
                .remote
                .as_ref()
                .expect("remote row after delete observation")
                .observed_seq,
            expected_remote_synced_seq: expect.remote_state.observed_seq,
        },
    );
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_late_bind_invariant_trace_matches_checked_in_artifact() {
    let report = run_late_bind_invariant_report();

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_binds_local_only_create.txt"
        )
    );
}

#[test]
fn remote_observation_convergence_invariant_trace_matches_checked_in_artifact() {
    let report = run_convergence_invariant_report();

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_converges_bound_file_edit.txt"
        )
    );
}

#[test]
fn remote_observation_ambiguous_bind_invariant_trace_matches_checked_in_artifact() {
    let report = run_ambiguous_bind_invariant_report();

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_ambiguous_bind_records_issue.txt"
        )
    );
}

#[test]
fn remote_observation_remote_rename_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_rename_applies_locally.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_file_remote_rename_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_move_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_move_applies_locally.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_file_remote_move_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_rename_failure_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_rename_invariant_report(
        "client/client_remote_observation_bound_file_remote_rename_destination_occupied_records_issue.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_file_remote_rename_destination_occupied_records_issue.txt"
        )
    );
}

#[test]
fn remote_observation_remote_delete_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_delete_invariant_report(
        "client/client_remote_observation_bound_file_remote_delete_applies_locally.yaml",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_file_remote_delete_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_delete_failure_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_delete_invariant_report(
        "client/client_remote_observation_bound_file_remote_delete_current_path_missing_records_issue.yaml",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_file_remote_delete_current_path_missing_records_issue.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_delete_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_delete_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_delete_applies_locally.yaml",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_directory_remote_subtree_delete_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_delete_failure_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_delete_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_delete_current_path_missing_records_issue.yaml",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_directory_remote_subtree_delete_current_path_missing_records_issue.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_delete_deferred_conflict_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_delete_conflict_report(
        "client/client_remote_observation_deletes_bound_directory_while_descendant_differs_from_anchor.yaml",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_deletes_bound_directory_while_descendant_differs_from_anchor.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_rename_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_rename_applies_locally.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_directory_remote_subtree_rename_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_move_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_move_applies_locally.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_directory_remote_subtree_move_applies_locally.txt"
        )
    );
}

#[test]
fn remote_observation_remote_subtree_rename_failure_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_subtree_rename_invariant_report(
        "client/client_remote_observation_bound_directory_remote_subtree_rename_destination_occupied_records_issue.yaml",
        false,
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_bound_directory_remote_subtree_rename_destination_occupied_records_issue.txt"
        )
    );
}

#[test]
fn remote_observation_active_upload_invariant_passes() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_bound_file_while_upload_transfer_active.yaml",
    );
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-upload-invariants");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);
    let observe = actions[0].apply().expect("apply action first");
    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let report = evaluate_remote_observation_active_upload_invariants(
        RemoteObservationActiveUploadInvariantInputs {
            transfer_present_after: db
                .load_transfer_ledger_for_inode(
                    &initial.remote_state.namespace_id,
                    initial.remote_state.inode_id,
                    TransferDirection::Upload,
                )
                .expect("load upload transfer ledger after observation")
                .is_some(),
            pending_inode_mutation_present_after: db
                .load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
                .expect("load pending inode mutation after observation")
                .is_some(),
            remote_synced_seq_after: expect.remote_state.observed_seq,
            expected_remote_synced_seq: observe.remote_observation.observed_seq,
        },
    );

    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after observation"),
        initial.local_file.content_utf8
    );
}

#[test]
fn remote_observation_active_download_invariant_passes() {
    let scenario = load_fixture(
        "client/client_remote_observation_updates_remote_only_file_while_download_transfer_active.yaml",
    );
    let initial: DownloadObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DownloadObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-active-download-invariants");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_remote_only_observation_state(&db_path, &initial);
    let observe = actions[0].apply().expect("apply action first");
    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let report = evaluate_remote_observation_active_download_invariants(
        RemoteObservationActiveDownloadInvariantInputs {
            transfer_present_after: db
                .load_transfer_ledger_for_inode(
                    &initial.remote_state.namespace_id,
                    initial.remote_state.inode_id,
                    TransferDirection::Download,
                )
                .expect("load download transfer ledger after observation")
                .is_some(),
            remote_synced_seq_after: expect.remote_state.observed_seq,
            expected_remote_synced_seq: observe.remote_observation.observed_seq,
        },
    );

    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

#[test]
fn remote_observation_converged_inode_late_response_is_idempotent() {
    let scenario = load_fixture(
        "client/client_remote_observation_converged_inode_late_response_is_idempotent.yaml",
    );
    let initial: LateResponseObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<LateResponseObservationAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: LateResponseObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-late-response");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    let local_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(
        &db_path,
        &EditObservationInitial {
            remote_state: initial.remote_state.clone(),
            local_state: initial.local_state.clone(),
            sync_anchor: initial.sync_anchor.clone(),
            local_file: initial.local_file.clone(),
            planned_action: initial.planned_action.clone(),
            pending_inode_mutation: initial.pending_inode_mutation.clone(),
            transfer_ledger: None,
        },
    );

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let response = actions[2]
        .apply_inode_mutation_response()
        .expect("late response should be third");
    assert!(actions[3].is_restart(), "restart should be fourth");
    let planner_tick = actions[4].planner().expect("planner should be fifth");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let applied = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB before late response");
        db.apply_inode_mutation_response(&response.response)
            .expect("late response should be idempotent")
    };

    assert_eq!(applied, expect.applied_inode_mutation);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after late response");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after late response");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views after late response"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
            .expect("load pending inode mutation after late response"),
        None
    );
    assert_eq!(
        fs::read_to_string(&local_path).expect("read local file after late response"),
        initial.local_file.content_utf8
    );
}

#[test]
fn remote_observation_bound_local_only_late_create_response_is_idempotent() {
    let scenario = load_fixture(
        "client/client_remote_observation_bound_local_only_late_create_response_is_idempotent.yaml",
    );
    let initial: LateCreateResponseObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<LateCreateResponseObservationAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: LateCreateResponseObservationExpect =
        scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-late-create-response");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let source_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    seed_create_observation_state(
        &db_path,
        &CreateObservationInitial {
            local_only_state: initial.local_only_state.clone(),
            local_file: initial.local_file.clone(),
            planned_local_only_action: initial.planned_local_only_action.clone(),
            pending_client_mutation: initial.pending_client_mutation.clone(),
        },
        &store,
        &source_path,
    );

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let response = actions[2]
        .apply_client_mutation_response()
        .expect("late create response should be third");
    assert!(actions[3].is_restart(), "restart should be fourth");
    let planner_tick = actions[4].planner().expect("planner should be fifth");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let applied = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB before late response");
        db.apply_client_mutation_response(&response.response)
            .expect("late create response should be idempotent")
    };

    assert_eq!(applied, expect.bound_identity);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after late response");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan inode after late response");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views after late create response"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&initial.pending_client_mutation.client_request_id)
            .expect("load pending client mutation after late create response"),
        if expect.pending_client_mutation_cleared {
            None
        } else {
            Some(initial.pending_client_mutation.clone().into_row())
        }
    );
    assert_eq!(
        fs::read_to_string(&source_path).expect("read local file after late create response"),
        initial.local_file.content_utf8
    );
}

fn seed_create_observation_state(
    db_path: &Path,
    initial: &CreateObservationInitial,
    store: &LocalFsStore,
    source_path: &Path,
) {
    let pending_client_mutation = initial.pending_client_mutation.clone().into_row();
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-create-observation-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        Ok(())
    })
    .expect("seed create observation local-only state");

    let uploaded =
        upload_small_file_from_path(store, &initial.local_only_state.namespace_id, source_path)
            .expect("upload local-only file for upload ledger");
    db.record_local_only_upload(
        &initial.local_only_state.client_file_id,
        &uploaded,
        pending_client_mutation.created_at_ms,
    )
    .expect("record local-only upload");
    db.record_pending_client_mutation(
        &initial.local_only_state.client_file_id,
        &pending_client_mutation.request,
        pending_client_mutation.created_at_ms,
    )
    .expect("record pending client mutation");
}

fn seed_edit_observation_state(db_path: &Path, initial: &EditObservationInitial) {
    let pending_inode_mutation = initial.pending_inode_mutation.clone().into_row();
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-edit-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        tx.upsert_planned_action(&initial.planned_action)?;
        Ok(())
    })
    .expect("seed edit observation state");
    db.record_pending_inode_mutation(
        &pending_inode_mutation.namespace_id,
        pending_inode_mutation.inode_id,
        &pending_inode_mutation.request,
        pending_inode_mutation.created_at_ms,
    )
    .expect("record pending inode mutation");
    if let Some(transfer_ledger) = &initial.transfer_ledger {
        db.upsert_transfer_ledger(&TransferLedgerRow {
            namespace_id: initial.remote_state.namespace_id.clone(),
            inode_id: initial.remote_state.inode_id,
            transfer_id: format!(
                "upload:{}:{}:{}",
                initial.remote_state.namespace_id.as_str(),
                initial.remote_state.inode_id.0,
                match &pending_inode_mutation.request.op {
                    ClientMutationOp::ReplaceFile {
                        content_manifest_digest,
                        ..
                    } => content_manifest_digest,
                    _ => unreachable!("pending inode mutation fixture should be replace_file"),
                }
            ),
            direction: transfer_ledger.direction,
            object_key: content_manifest(
                initial.remote_state.namespace_id.as_str(),
                match &pending_inode_mutation.request.op {
                    ClientMutationOp::ReplaceFile {
                        content_manifest_digest,
                        ..
                    } => content_manifest_digest,
                    _ => unreachable!("pending inode mutation fixture should be replace_file"),
                },
            ),
            block_index: transfer_ledger.block_index,
            block_count: transfer_ledger.block_count,
            state: transfer_ledger.state,
            updated_at_ms: transfer_ledger.updated_at_ms,
        })
        .expect("record active transfer ledger");
    }
}

fn seed_remote_only_observation_state(db_path: &Path, initial: &DownloadObservationInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-only-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_planned_action(&initial.planned_action)?;
        Ok(())
    })
    .expect("seed remote-only observation state");
    db.upsert_transfer_ledger(&TransferLedgerRow {
        namespace_id: initial.remote_state.namespace_id.clone(),
        inode_id: initial.remote_state.inode_id,
        transfer_id: format!(
            "download:{}:{}:{}",
            initial.remote_state.namespace_id.as_str(),
            initial.remote_state.inode_id.0,
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("remote-only observation fixture should include manifest digest"),
        ),
        direction: initial.transfer_ledger.direction,
        object_key: content_manifest(
            initial.remote_state.namespace_id.as_str(),
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("remote-only observation fixture should include manifest digest"),
        ),
        block_index: initial.transfer_ledger.block_index,
        block_count: initial.transfer_ledger.block_count,
        state: initial.transfer_ledger.state,
        updated_at_ms: initial.transfer_ledger.updated_at_ms,
    })
    .expect("record active download transfer ledger");
}

fn seed_bound_download_observation_state(
    db_path: &Path,
    initial: &BoundDownloadObservationInitial,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-bound-download-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        tx.upsert_planned_action(&initial.planned_action)?;
        Ok(())
    })
    .expect("seed bound download observation state");
    db.upsert_transfer_ledger(&TransferLedgerRow {
        namespace_id: initial.remote_state.namespace_id.clone(),
        inode_id: initial.remote_state.inode_id,
        transfer_id: format!(
            "download:{}:{}:{}",
            initial.remote_state.namespace_id.as_str(),
            initial.remote_state.inode_id.0,
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("bound download fixture should include manifest digest"),
        ),
        direction: initial.transfer_ledger.direction,
        object_key: content_manifest(
            initial.remote_state.namespace_id.as_str(),
            initial
                .remote_state
                .content_manifest_digest
                .as_deref()
                .expect("bound download fixture should include manifest digest"),
        ),
        block_index: initial.transfer_ledger.block_index,
        block_count: initial.transfer_ledger.block_count,
        state: initial.transfer_ledger.state,
        updated_at_ms: initial.transfer_ledger.updated_at_ms,
    })
    .expect("record bound download transfer ledger");
}

fn seed_remote_delete_observation_state(db_path: &Path, initial: &DeleteObservationInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-delete-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        Ok(())
    })
    .expect("seed remote delete observation state");
}

fn seed_ambiguous_observation_state(db_path: &Path, local_only_state: &[LocalOnlyFileStateRow]) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-ambiguous-observation-state", |tx| {
        for row in local_only_state {
            tx.upsert_local_only_file(row)?;
        }
        Ok(())
    })
    .expect("seed ambiguous local-only state");
}

fn write_source_file(source_root: &Path, relative_path: &Path, content_utf8: &str) -> PathBuf {
    let source_path = source_root.join(relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, content_utf8.as_bytes()).expect("write source file");
    source_path
}

fn run_late_bind_invariant_report() -> ReconciliationInvariantFixtureRunReport {
    let scenario = load_fixture("client/client_remote_observation_binds_local_only_create.yaml");
    let initial: CreateObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: CreateObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-late-bind-invariants");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let source_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    seed_create_observation_state(&db_path, &initial, &store, &source_path);
    let observe = actions[0].apply().expect("apply action first");
    let planner_tick = actions[2].planner().expect("planner action third");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load converged views");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan bound inode after restart");
    let report = evaluate_remote_observation_late_bind_invariants(
        RemoteObservationLateBindInvariantInputs {
            remote_present_after: views.remote.is_some(),
            local_present_after: views.local.is_some(),
            sync_anchor_present_after: views.sync_anchor.is_some(),
            local_dirty_after: views.local.as_ref().is_some_and(|local| local.dirty),
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|remote| remote.content_digest.as_deref()),
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|local| local.content_digest.as_deref()),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|anchor| anchor.content_digest.as_deref()),
            local_only_file_present_after: db
                .load_local_only_file(&initial.local_only_state.client_file_id)
                .expect("load local-only file after bind")
                .is_some(),
            planned_local_only_action_present_after: db
                .load_planned_local_only_action(&initial.local_only_state.client_file_id)
                .expect("load local-only plan after bind")
                .is_some(),
            local_only_upload_present_after: db
                .load_local_only_upload(&initial.local_only_state.client_file_id)
                .expect("load local-only upload after bind")
                .is_some(),
            local_only_transfer_present_after: db
                .load_local_only_transfer_ledger(
                    &initial.local_only_state.client_file_id,
                    TransferDirection::Upload,
                )
                .expect("load local-only transfer ledger after bind")
                .is_some(),
            local_only_issue_count_after: db
                .load_local_only_conflicts_and_errors(&initial.local_only_state.client_file_id)
                .expect("load local-only issues after bind")
                .len(),
            pending_client_mutation_present_after: db
                .load_pending_client_mutation(&initial.pending_client_mutation.client_request_id)
                .expect("load pending client mutation after bind")
                .is_some(),
        },
    );

    assert_eq!(planner_result, expect.planner_result);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("planner_result={planner_result:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("late-bind"))
    .collect::<Vec<_>>();

    ReconciliationInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn run_convergence_invariant_report() -> ReconciliationInvariantFixtureRunReport {
    let scenario = load_fixture("client/client_remote_observation_converges_bound_file_edit.yaml");
    let initial: EditObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: EditObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-convergence-invariants");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");
    write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );

    seed_edit_observation_state(&db_path, &initial);
    let observe = actions[0].apply().expect("apply action first");
    let planner_tick = actions[2].planner().expect("planner action third");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let views = db
        .load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
        .expect("load converged views");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan bound inode after restart");
    let report = evaluate_remote_observation_convergence_invariants(
        RemoteObservationConvergenceInvariantInputs {
            planned_action_present_after: db
                .load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
                .expect("load planned action after convergence")
                .is_some(),
            pending_inode_mutation_present_after: db
                .load_pending_inode_mutation(&initial.pending_inode_mutation.client_request_id)
                .expect("load pending inode mutation after convergence")
                .is_some(),
            local_dirty_after: views.local.as_ref().is_some_and(|local| local.dirty),
            local_content_digest_after: views
                .local
                .as_ref()
                .and_then(|local| local.content_digest.as_deref()),
            remote_synced_seq_after: views
                .remote
                .as_ref()
                .expect("converged remote after observation")
                .observed_seq,
            remote_revision_no_after: views
                .remote
                .as_ref()
                .expect("converged remote after observation")
                .revision_no,
            remote_content_digest_after: views
                .remote
                .as_ref()
                .and_then(|remote| remote.content_digest.as_deref()),
            sync_anchor_seq_after: views.sync_anchor.as_ref().map(|anchor| anchor.synced_seq),
            sync_anchor_revision_no_after: views
                .sync_anchor
                .as_ref()
                .map(|anchor| anchor.revision_no),
            sync_anchor_content_digest_after: views
                .sync_anchor
                .as_ref()
                .and_then(|anchor| anchor.content_digest.as_deref()),
        },
    );

    assert_eq!(planner_result, expect.planner_result);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("planner_result={planner_result:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("bound-convergence"))
    .collect::<Vec<_>>();

    ReconciliationInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn run_ambiguous_bind_invariant_report() -> ReconciliationInvariantFixtureRunReport {
    let scenario =
        load_fixture("client/client_remote_observation_ambiguous_bind_records_issue.yaml");
    let initial: AmbiguousObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: AmbiguousObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-ambiguous-invariants");
    let db_path = temp_dir.path().join("client.sqlite3");

    seed_ambiguous_observation_state(&db_path, &initial.local_only_state);
    let observe = actions[0].apply().expect("apply action first");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let issues = db
        .load_conflicts_and_errors(&NamespaceId::from("ns-1"), InodeId(601))
        .expect("load ambiguous issue rows");
    let report = evaluate_remote_observation_ambiguous_bind_invariants(
        RemoteObservationAmbiguousBindInvariantInputs {
            issue_kind_after: issues.first().map(|issue| issue.kind.as_str()),
            issue_matches_after: issues
                .first()
                .and_then(|issue| issue.detail_json["matches"].as_u64())
                .map(|matches| matches as usize),
            remote_present_after: db
                .load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
                .expect("load file sync views after ambiguous observation")
                .remote
                .is_some(),
            local_present_after: db
                .load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
                .expect("load file sync views after ambiguous observation")
                .local
                .is_some(),
            sync_anchor_present_after: db
                .load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(601))
                .expect("load file sync views after ambiguous observation")
                .sync_anchor
                .is_some(),
            surviving_local_only_count_after: initial
                .local_only_state
                .iter()
                .filter(|row| {
                    db.load_local_only_file(&row.client_file_id)
                        .expect("load local-only row after ambiguous observation")
                        .is_some()
                })
                .count(),
            initial_local_only_count: initial.local_only_state.len(),
        },
    );

    assert_eq!(issues.len(), expect.conflicts_and_errors.len());
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("issues_after={issues:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("ambiguous-bind"))
    .collect::<Vec<_>>();

    ReconciliationInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn run_remote_rename_invariant_report(
    fixture_path: &str,
    retry_after_failure: bool,
) -> RenameInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: RenameObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteRenameFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: RenameObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-rename");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let current_path = write_source_file(
        &source_root,
        &initial.local_file.relative_path,
        &initial.local_file.content_utf8,
    );
    let target_path = source_root.join(&initial.target_relative_path);
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");
    if let Some(content_utf8) = &initial.occupied_target_content_utf8 {
        fs::write(&target_path, content_utf8).expect("seed occupied target");
    }

    seed_remote_rename_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let first_planner_tick = actions[2].planner().expect("planner action third");
    let execute = actions[3]
        .execute_next_client_action()
        .expect("execute action fourth");
    assert!(actions[4].is_restart(), "restart should be fifth");
    let second_planner_tick = actions[5].planner().expect("planner action sixth");

    let observation_outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(observation_outcome, expect.outcome.clone().into_outcome());

    let first_planner_result = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
        plan_file(
            &mut db,
            &first_planner_tick.namespace_id,
            first_planner_tick.inode_id,
            first_planner_tick.now_ms,
        )
        .expect("plan bound inode after observation")
    };
    assert_eq!(first_planner_result, expect.first_planner_result);

    let executed = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for rename execute");
        execute_next_client_action(
            &mut db,
            &store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| panic!("apply_remote_rename should not dispatch a mutation request"),
        )
    };
    assert_rename_execution_matches_expectation(executed, &expect.execution);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after rename execute");
    let second_planner_result = plan_file(
        &mut db,
        &second_planner_tick.namespace_id,
        second_planner_tick.inode_id,
        second_planner_tick.now_ms,
    )
    .expect("plan bound inode after rename execute");
    assert_eq!(second_planner_result, expect.second_planner_result);

    let views = db
        .load_file_sync_views(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load file sync views after rename execute");
    assert_eq!(
        views,
        loon_client::state_db::FileSyncViews {
            namespace_id: second_planner_tick.namespace_id.clone(),
            inode_id: second_planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    let actual_planned_action = db
        .load_planned_action(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load planned action after rename execute");
    if expect.planned_action_cleared {
        assert_eq!(actual_planned_action, None);
    } else {
        let actual_planned_action = actual_planned_action
            .as_ref()
            .expect("planned action should remain after failed rename");
        assert_eq!(
            actual_planned_action.decision,
            expect.second_planner_result.decision.as_str()
        );
        assert_eq!(
            actual_planned_action.reason,
            expect.second_planner_result.reason.as_str()
        );
    }
    let issues = db
        .load_conflicts_and_errors(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load rename issues after execute")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(issues, expect.conflicts_and_errors);

    let planning_report =
        evaluate_remote_path_change_planning_invariants(RemotePathChangePlanningInvariantInputs {
            planned_action_decision_after: Some(expect.first_planner_result.decision.as_str()),
            planned_action_reason_after: Some(expect.first_planner_result.reason.as_str()),
        });
    let apply_report = evaluate_apply_remote_rename_invariants(ApplyRemoteRenameInvariantInputs {
        local_exists_on_disk_after: views
            .local
            .as_ref()
            .is_some_and(|local| local.exists_on_disk),
        local_dirty_after: views.local.as_ref().is_some_and(|local| local.dirty),
        local_parent_inode_after: views.local.as_ref().and_then(|local| local.parent_inode_id),
        local_display_name_after: views
            .local
            .as_ref()
            .expect("local row after rename execute")
            .display_name
            .as_str(),
        remote_synced_seq_after: views
            .remote
            .as_ref()
            .expect("remote row after rename execute")
            .observed_seq,
        remote_revision_no_after: views
            .remote
            .as_ref()
            .expect("remote row after rename execute")
            .revision_no,
        remote_content_digest_after: views
            .remote
            .as_ref()
            .and_then(|remote| remote.content_digest.as_deref()),
        remote_parent_inode_after: views
            .remote
            .as_ref()
            .and_then(|remote| remote.parent_inode_id),
        remote_display_name_after: views
            .remote
            .as_ref()
            .expect("remote row after rename execute")
            .display_name
            .as_str(),
        sync_anchor_seq_after: views.sync_anchor.as_ref().map(|anchor| anchor.synced_seq),
        sync_anchor_revision_no_after: views.sync_anchor.as_ref().map(|anchor| anchor.revision_no),
        sync_anchor_content_digest_after: views
            .sync_anchor
            .as_ref()
            .and_then(|anchor| anchor.content_digest.as_deref()),
        sync_anchor_parent_inode_after: views
            .sync_anchor
            .as_ref()
            .and_then(|anchor| anchor.parent_inode_id),
        sync_anchor_display_name_after: views
            .sync_anchor
            .as_ref()
            .map(|anchor| anchor.display_name.as_str()),
        planned_action_present_after: actual_planned_action.is_some(),
        issue_kind_after: issues.first().map(|issue| issue.kind.as_str()),
    });
    let report = combine_reconciliation_reports([planning_report, apply_report]);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("first_planner_result={first_planner_result:?}"),
        format!("execution={:?}", expect.execution),
        format!("second_planner_result={second_planner_result:?}"),
        format!("issues_after={issues:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("remote-rename"))
    .collect::<Vec<_>>();
    let rendered_trace = render_trace(&scenario, &trace);

    let mut final_current_exists = current_path.exists();
    let mut final_target_exists = target_path.exists();
    if retry_after_failure
        && matches!(
            expect.execution,
            RawRenameExecutionExpect::ExecuteErrorKind { .. }
        )
    {
        if target_path.exists() {
            fs::remove_file(&target_path).expect("remove occupied target before retry");
        }
        let retried = {
            let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for retry");
            execute_next_client_action(
                &mut db,
                &store,
                |_client_file_id| None,
                |_namespace_id, _inode_id| Some(current_path.clone()),
                |_namespace_id, _inode_id, _parent_inode_id, _display_name| {
                    Some(target_path.clone())
                },
                execute.uploaded_at_ms,
                execute.created_at_ms.saturating_add(1),
                |_request| panic!("apply_remote_rename should not dispatch a mutation request"),
            )
            .expect("retry execute_next_client_action after clearing destination")
        };
        match retried {
            Some(NextClientAction::ExecutedApplyRemoteRename(ExecutedApplyRemoteRename {
                ..
            })) => {}
            other => panic!("expected retry to apply remote rename, got {other:?}"),
        }
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after retry");
        let retry_planner_result = plan_file(
            &mut db,
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
            second_planner_tick.now_ms.saturating_add(1),
        )
        .expect("plan inode after retry");
        assert_eq!(
            retry_planner_result.decision.as_str(),
            "no_op",
            "retry should converge after clearing destination"
        );
        final_current_exists = current_path.exists();
        final_target_exists = target_path.exists();
    }

    RenameInvariantFixtureRunReport {
        rendered_trace,
        final_current_exists,
        final_target_exists,
    }
}

fn run_remote_delete_invariant_report(fixture_path: &str) -> DeleteInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: DeleteObservationInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteRenameFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: DeleteObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-delete");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    let current_path = source_root.join(&initial.local_file.relative_path);
    fs::create_dir_all(current_path.parent().expect("current path parent"))
        .expect("create current path parent");
    if initial.seed_local_file {
        fs::write(&current_path, initial.local_file.content_utf8.as_bytes())
            .expect("write current local file");
    }

    seed_remote_delete_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let first_planner_tick = actions[2].planner().expect("planner action third");
    let execute = actions[3]
        .execute_next_client_action()
        .expect("execute action fourth");
    assert!(actions[4].is_restart(), "restart should be fifth");
    let second_planner_tick = actions[5].planner().expect("planner action sixth");

    let observation_outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(observation_outcome, expect.outcome.clone().into_outcome());

    let first_planner_result = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
        plan_file(
            &mut db,
            &first_planner_tick.namespace_id,
            first_planner_tick.inode_id,
            first_planner_tick.now_ms,
        )
        .expect("plan bound inode after observation")
    };
    assert_eq!(first_planner_result, expect.first_planner_result);

    let executed = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for delete execute");
        execute_next_client_action(
            &mut db,
            &store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(current_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| panic!("apply_remote_delete should not dispatch a mutation request"),
        )
    };
    assert_delete_execution_matches_expectation(executed, &expect.execution);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after delete execute");
    let second_planner_result = plan_file(
        &mut db,
        &second_planner_tick.namespace_id,
        second_planner_tick.inode_id,
        second_planner_tick.now_ms,
    )
    .expect("plan bound inode after delete execute");
    assert_eq!(second_planner_result, expect.second_planner_result);

    let views = db
        .load_file_sync_views(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load file sync views after delete execute");
    assert_eq!(views.remote, Some(expect.remote_state.clone()));
    assert_eq!(views.local, expect.local_state.clone());
    assert_eq!(views.sync_anchor, expect.sync_anchor.clone());

    let actual_planned_action = db
        .load_planned_action(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load planned action after delete execute");
    if expect.planned_action_cleared {
        assert_eq!(actual_planned_action, None);
    } else {
        let actual_planned_action = actual_planned_action
            .as_ref()
            .expect("planned action should remain after failed delete");
        assert_eq!(
            actual_planned_action.decision,
            expect.second_planner_result.decision.as_str()
        );
        assert_eq!(
            actual_planned_action.reason,
            expect.second_planner_result.reason.as_str()
        );
    }

    let issues = db
        .load_conflicts_and_errors(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load remote delete issues after execute")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(issues, expect.conflicts_and_errors);

    let planning_report =
        evaluate_remote_delete_planning_invariants(RemoteDeletePlanningInvariantInputs {
            planned_action_decision_after: Some(first_planner_result.decision.as_str()),
            planned_action_reason_after: Some(first_planner_result.reason.as_str()),
        });
    let apply_report = evaluate_apply_remote_delete_invariants(ApplyRemoteDeleteInvariantInputs {
        remote_present_after: views.remote.is_some(),
        remote_is_deleted_after: views
            .remote
            .as_ref()
            .is_some_and(|remote| remote.is_deleted),
        local_present_after: views.local.is_some(),
        sync_anchor_present_after: views.sync_anchor.is_some(),
        planned_action_present_after: actual_planned_action.is_some(),
        issue_kind_after: issues.first().map(|issue| issue.kind.as_str()),
    });
    let report = combine_reconciliation_reports([planning_report, apply_report]);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("first_planner_result={first_planner_result:?}"),
        format!("execution={:?}", expect.execution),
        format!("second_planner_result={second_planner_result:?}"),
        format!("issues_after={issues:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("remote-delete"))
    .collect::<Vec<_>>();

    DeleteInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
        final_current_exists: current_path.exists(),
    }
}

fn run_remote_subtree_delete_invariant_report(
    fixture_path: &str,
) -> SubtreeDeleteInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: SubtreeDeleteObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteRenameFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: SubtreeDeleteObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-subtree-delete");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    materialize_local_tree(&source_root, &initial.local_entries);
    let root_path = source_root.join(&initial.root_relative_path);
    seed_remote_subtree_delete_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let first_planner_tick = actions[2].planner().expect("planner action third");
    let execute = actions[3]
        .execute_next_client_action()
        .expect("execute action fourth");
    assert!(actions[4].is_restart(), "restart should be fifth");
    let second_planner_tick = actions[5].planner().expect("planner action sixth");

    let observation_outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(observation_outcome, expect.outcome.clone().into_outcome());

    let first_planner_result = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
        plan_file(
            &mut db,
            &first_planner_tick.namespace_id,
            first_planner_tick.inode_id,
            first_planner_tick.now_ms,
        )
        .expect("plan bound subtree root after observation")
    };
    assert_eq!(first_planner_result, expect.first_planner_result);

    let executed = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for subtree delete execute");
        execute_next_client_action(
            &mut db,
            &store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(root_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| None,
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| panic!("apply_remote_subtree_delete should not dispatch a mutation request"),
        )
    };
    assert_subtree_delete_execution_matches_expectation(executed, &expect.execution);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after subtree delete execute");
    let second_planner_result = plan_file(
        &mut db,
        &second_planner_tick.namespace_id,
        second_planner_tick.inode_id,
        second_planner_tick.now_ms,
    )
    .expect("plan bound subtree root after execute");
    assert_eq!(second_planner_result, expect.second_planner_result);

    assert_subtree_states_match(&db, &initial, &expect);

    let issues = db
        .load_conflicts_and_errors(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load subtree delete issues after execute")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(issues, expect.conflicts_and_errors);

    for path_expectation in &expect.path_expectations {
        assert_eq!(
            source_root.join(&path_expectation.relative_path).exists(),
            path_expectation.exists,
            "unexpected path existence for {}",
            path_expectation.relative_path.display()
        );
    }

    let descendant_inode_ids = initial
        .remote_state
        .iter()
        .filter(|row| row.inode_id != second_planner_tick.inode_id)
        .map(|row| row.inode_id)
        .collect::<BTreeSet<_>>();
    let subtree_inode_ids =
        rooted_subtree_inode_ids(&initial.local_state, second_planner_tick.inode_id);
    let descendant_remote_count_after = descendant_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load descendant views")
                .remote
                .is_some()
        })
        .count();
    let subtree_local_state_count_after = subtree_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load subtree local views")
                .local
                .is_some()
        })
        .count();
    let subtree_sync_anchor_count_after = subtree_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load subtree anchor views")
                .sync_anchor
                .is_some()
        })
        .count();
    let subtree_planned_action_count_after = subtree_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_planned_action(&second_planner_tick.namespace_id, **inode_id)
                .expect("load subtree planned action")
                .is_some()
        })
        .count();

    let planning_report = evaluate_remote_subtree_delete_planning_invariants(
        RemoteSubtreeDeletePlanningInvariantInputs {
            planned_action_decision_after: Some(first_planner_result.decision.as_str()),
            planned_action_reason_after: Some(first_planner_result.reason.as_str()),
        },
    );
    let root_views = db
        .load_file_sync_views(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load root views after subtree delete execute");
    let apply_report =
        evaluate_apply_remote_subtree_delete_invariants(ApplyRemoteSubtreeDeleteInvariantInputs {
            root_remote_present_after: root_views.remote.is_some(),
            root_remote_is_deleted_after: root_views
                .remote
                .as_ref()
                .is_some_and(|remote| remote.is_deleted),
            descendant_remote_count_after,
            subtree_local_state_count_after,
            subtree_sync_anchor_count_after,
            subtree_planned_action_count_after,
            issue_kind_after: issues.first().map(|issue| issue.kind.as_str()),
        });
    let report = combine_reconciliation_reports([planning_report, apply_report]);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("first_planner_result={first_planner_result:?}"),
        format!("execution={:?}", expect.execution),
        format!("second_planner_result={second_planner_result:?}"),
        format!("issues_after={issues:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("remote-subtree-delete"))
    .collect::<Vec<_>>();

    SubtreeDeleteInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
        final_root_exists: root_path.exists(),
    }
}

fn run_remote_subtree_delete_conflict_report(
    fixture_path: &str,
) -> SubtreeDeleteInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: SubtreeDeleteObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: SubtreeDeleteConflictObservationExpect =
        scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-subtree-delete-conflict");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");

    materialize_local_tree(&source_root, &initial.local_entries);
    let root_path = source_root.join(&initial.root_relative_path);
    seed_remote_subtree_delete_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan subtree delete conflict after restart");
    assert_eq!(planner_result, expect.planner_result);

    for path_expectation in &expect.path_expectations {
        assert_eq!(
            source_root.join(&path_expectation.relative_path).exists(),
            path_expectation.exists,
            "unexpected path existence for {}",
            path_expectation.relative_path.display()
        );
    }

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("planner_result={planner_result:?}"),
    ];

    SubtreeDeleteInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
        final_root_exists: root_path.exists(),
    }
}

fn run_remote_subtree_rename_invariant_report(
    fixture_path: &str,
    retry_after_failure: bool,
) -> SubtreeRenameInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: SubtreeRenameObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteRenameFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: SubtreeRenameObservationExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-subtree-rename");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create objectstore root");
    fs::create_dir_all(&source_root).expect("create source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    materialize_local_tree(&source_root, &initial.local_entries);
    let root_path = source_root.join(&initial.root_relative_path);
    let target_path = source_root.join(&initial.target_relative_path);
    fs::create_dir_all(target_path.parent().expect("target parent")).expect("create target parent");
    seed_remote_subtree_rename_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let first_planner_tick = actions[2].planner().expect("planner action third");
    let execute = actions[3]
        .execute_next_client_action()
        .expect("execute action fourth");
    assert!(actions[4].is_restart(), "restart should be fifth");
    let second_planner_tick = actions[5].planner().expect("planner action sixth");

    let observation_outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(observation_outcome, expect.outcome.clone().into_outcome());

    let first_planner_result = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
        plan_file(
            &mut db,
            &first_planner_tick.namespace_id,
            first_planner_tick.inode_id,
            first_planner_tick.now_ms,
        )
        .expect("plan bound subtree root after observation")
    };
    assert_eq!(first_planner_result, expect.first_planner_result);

    let executed = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for subtree rename execute");
        execute_next_client_action(
            &mut db,
            &store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| Some(root_path.clone()),
            |_namespace_id, _inode_id, _parent_inode_id, _display_name| Some(target_path.clone()),
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |_request| panic!("apply_remote_subtree_rename should not dispatch a mutation request"),
        )
    };
    assert_subtree_rename_execution_matches_expectation(executed, &expect.execution);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after subtree rename execute");
    let second_planner_result = plan_file(
        &mut db,
        &second_planner_tick.namespace_id,
        second_planner_tick.inode_id,
        second_planner_tick.now_ms,
    )
    .expect("plan bound subtree root after execute");
    assert_eq!(second_planner_result, expect.second_planner_result);

    assert_subtree_rename_states_match(&db, &initial, &expect);

    let issues = db
        .load_conflicts_and_errors(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load subtree rename issues after execute")
        .into_iter()
        .map(RawConflictOrErrorExpect::from_row)
        .collect::<Vec<_>>();
    assert_eq!(issues, expect.conflicts_and_errors);

    for path_expectation in &expect.path_expectations {
        assert_eq!(
            source_root.join(&path_expectation.relative_path).exists(),
            path_expectation.exists,
            "unexpected path existence for {}",
            path_expectation.relative_path.display()
        );
    }

    let subtree_inode_ids =
        rooted_subtree_inode_ids(&initial.local_state, second_planner_tick.inode_id);
    let descendant_inode_ids = subtree_inode_ids
        .iter()
        .copied()
        .filter(|inode_id| *inode_id != second_planner_tick.inode_id)
        .collect::<BTreeSet<_>>();
    let descendant_remote_count_after = descendant_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load descendant remote views")
                .remote
                .is_some()
        })
        .count();
    let descendant_local_state_count_after = descendant_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load descendant local views")
                .local
                .is_some()
        })
        .count();
    let descendant_sync_anchor_count_after = descendant_inode_ids
        .iter()
        .filter(|inode_id| {
            db.load_file_sync_views(&second_planner_tick.namespace_id, **inode_id)
                .expect("load descendant anchor views")
                .sync_anchor
                .is_some()
        })
        .count();
    let expected_descendant_remote_count = expect
        .remote_state_after
        .iter()
        .filter(|row| descendant_inode_ids.contains(&row.inode_id))
        .count();
    let expected_descendant_local_state_count = expect
        .local_state_after
        .iter()
        .filter(|row| descendant_inode_ids.contains(&row.inode_id))
        .count();
    let expected_descendant_sync_anchor_count = expect
        .sync_anchor_after
        .iter()
        .filter(|row| descendant_inode_ids.contains(&row.inode_id))
        .count();

    let planning_report = evaluate_remote_subtree_rename_planning_invariants(
        RemoteSubtreeRenamePlanningInvariantInputs {
            planned_action_decision_after: Some(first_planner_result.decision.as_str()),
            planned_action_reason_after: Some(first_planner_result.reason.as_str()),
        },
    );
    let root_views = db
        .load_file_sync_views(
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
        )
        .expect("load root views after subtree rename execute");
    let root_remote = root_views
        .remote
        .as_ref()
        .expect("root remote row after subtree rename execute");
    let root_local = root_views
        .local
        .as_ref()
        .expect("root local row after subtree rename execute");
    let apply_report =
        evaluate_apply_remote_subtree_rename_invariants(ApplyRemoteSubtreeRenameInvariantInputs {
            root_local_exists_on_disk_after: root_local.exists_on_disk,
            root_local_dirty_after: root_local.dirty,
            root_local_parent_inode_after: root_local.parent_inode_id,
            root_local_display_name_after: root_local.display_name.as_str(),
            root_remote_synced_seq_after: root_remote.observed_seq,
            root_remote_revision_no_after: root_remote.revision_no,
            root_remote_parent_inode_after: root_remote.parent_inode_id,
            root_remote_display_name_after: root_remote.display_name.as_str(),
            root_sync_anchor_seq_after: root_views
                .sync_anchor
                .as_ref()
                .map(|anchor| anchor.synced_seq),
            root_sync_anchor_revision_no_after: root_views
                .sync_anchor
                .as_ref()
                .map(|anchor| anchor.revision_no),
            root_sync_anchor_parent_inode_after: root_views
                .sync_anchor
                .as_ref()
                .and_then(|anchor| anchor.parent_inode_id),
            root_sync_anchor_display_name_after: root_views
                .sync_anchor
                .as_ref()
                .map(|anchor| anchor.display_name.as_str()),
            descendant_remote_count_after,
            expected_descendant_remote_count,
            descendant_local_state_count_after,
            expected_descendant_local_state_count,
            descendant_sync_anchor_count_after,
            expected_descendant_sync_anchor_count,
            root_planned_action_present_after: db
                .load_planned_action(
                    &second_planner_tick.namespace_id,
                    second_planner_tick.inode_id,
                )
                .expect("load root planned action after subtree rename execute")
                .is_some(),
            issue_kind_after: issues.first().map(|issue| issue.kind.as_str()),
        });
    let report = combine_reconciliation_reports([planning_report, apply_report]);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("first_planner_result={first_planner_result:?}"),
        format!("execution={:?}", expect.execution),
        format!("second_planner_result={second_planner_result:?}"),
        format!("issues_after={issues:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("remote-subtree-rename"))
    .collect::<Vec<_>>();
    let rendered_trace = render_trace(&scenario, &trace);

    let mut final_root_exists = root_path.exists();
    let mut final_target_exists = target_path.exists();
    if retry_after_failure
        && matches!(
            expect.execution,
            RawSubtreeRenameExecutionExpect::ExecuteErrorKind { .. }
        )
    {
        remove_local_path_if_present(&target_path);
        let retried = {
            let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for subtree rename retry");
            execute_next_client_action(
                &mut db,
                &store,
                |_client_file_id| None,
                |_namespace_id, _inode_id| Some(root_path.clone()),
                |_namespace_id, _inode_id, _parent_inode_id, _display_name| {
                    Some(target_path.clone())
                },
                execute.uploaded_at_ms,
                execute.created_at_ms.saturating_add(1),
                |_request| {
                    panic!("apply_remote_subtree_rename should not dispatch a mutation request")
                },
            )
            .expect("retry execute_next_client_action after clearing destination")
        };
        match retried {
            Some(NextClientAction::ExecutedApplyRemoteSubtreeRename(
                ExecutedApplyRemoteSubtreeRename { .. },
            )) => {}
            other => panic!("expected retry to apply remote subtree rename, got {other:?}"),
        }
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after subtree rename retry");
        let retry_planner_result = plan_file(
            &mut db,
            &second_planner_tick.namespace_id,
            second_planner_tick.inode_id,
            second_planner_tick.now_ms.saturating_add(1),
        )
        .expect("plan subtree root after retry");
        assert_eq!(retry_planner_result.decision.as_str(), "no_op");
        final_root_exists = root_path.exists();
        final_target_exists = target_path.exists();
    }

    SubtreeRenameInvariantFixtureRunReport {
        rendered_trace,
        final_root_exists,
        final_target_exists,
    }
}

fn run_remote_subtree_rename_conflict_report(
    fixture_path: &str,
) -> SubtreeRenameInvariantFixtureRunReport {
    let scenario = load_fixture(fixture_path);
    let initial: SubtreeRenameObservationInitial =
        scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: SubtreeRenameConflictObservationExpect =
        scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new("client-remote-observation-remote-subtree-rename-conflict");
    let db_path = temp_dir.path().join("client.sqlite3");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&source_root).expect("create source root");

    materialize_local_tree(&source_root, &initial.local_entries);
    let root_path = source_root.join(&initial.root_relative_path);
    seed_remote_subtree_rename_observation_state(&db_path, &initial);

    let observe = actions[0].apply().expect("apply action first");
    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };
    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_tick = actions[2].planner().expect("planner action third");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan subtree rename conflict after restart");
    assert_eq!(planner_result, expect.planner_result);

    for path_expectation in &expect.path_expectations {
        assert_eq!(
            source_root.join(&path_expectation.relative_path).exists(),
            path_expectation.exists,
            "unexpected path existence for {}",
            path_expectation.relative_path.display()
        );
    }

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("planner_result={planner_result:?}"),
    ];

    SubtreeRenameInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
        final_root_exists: root_path.exists(),
        final_target_exists: source_root.join(&initial.target_relative_path).exists(),
    }
}

fn materialize_local_tree(root: &Path, entries: &[FixtureLocalTreeEntry]) {
    for entry in entries {
        let path = root.join(&entry.relative_path);
        match entry.inode_kind {
            InodeKind::Dir => {
                fs::create_dir_all(&path).expect("create local directory");
            }
            InodeKind::File => {
                fs::create_dir_all(path.parent().expect("file parent"))
                    .expect("create local file parent");
                fs::write(&path, entry.content_utf8.as_deref().unwrap_or_default())
                    .expect("write local file");
            }
            InodeKind::Symlink | InodeKind::Mount => {
                panic!("fixture local tree currently supports only files and directories")
            }
        }
    }
}

fn seed_remote_subtree_delete_observation_state(
    db_path: &Path,
    initial: &SubtreeDeleteObservationInitial,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-subtree-delete-observation-state", |tx| {
        for row in &initial.remote_state {
            tx.upsert_remote_file(row)?;
        }
        for row in &initial.local_state {
            tx.upsert_local_file(row)?;
        }
        for row in &initial.sync_anchor {
            tx.upsert_sync_anchor(row)?;
        }
        for row in &initial.planned_actions {
            tx.upsert_planned_action(row)?;
        }
        for row in &initial.pending_inode_mutations {
            let row = row.clone().into_row();
            tx.record_pending_inode_mutation(
                &row.namespace_id,
                row.inode_id,
                &row.request,
                row.created_at_ms,
            )?;
        }
        for row in &initial.transfer_ledgers {
            tx.upsert_transfer_ledger(&row.clone().into_row())?;
        }
        Ok(())
    })
    .expect("seed remote subtree delete observation state");
}

fn assert_subtree_delete_execution_matches_expectation(
    result: Result<Option<NextClientAction>, ExecuteNextClientActionError>,
    expected: &RawSubtreeDeleteExecutionExpect,
) {
    match expected {
        RawSubtreeDeleteExecutionExpect::ExecutedApplyRemoteSubtreeDelete {
            executed_apply_remote_subtree_delete,
        } => {
            let executed = result.expect("subtree delete execution should succeed");
            assert_eq!(
                executed,
                Some(NextClientAction::ExecutedApplyRemoteSubtreeDelete(
                    ExecutedApplyRemoteSubtreeDelete {
                        applied: executed_apply_remote_subtree_delete.clone(),
                    },
                ))
            );
        }
        RawSubtreeDeleteExecutionExpect::ExecuteErrorKind { execute_error_kind } => {
            let error = result.expect_err("subtree delete execution should fail");
            let actual_kind = match error {
                ExecuteNextClientActionError::ApplyRemoteSubtreeDelete(
                    ExecuteApplyRemoteSubtreeDeleteError::CurrentPathMissing { .. },
                ) => "current_path_missing",
                ExecuteNextClientActionError::ApplyRemoteSubtreeDelete(
                    ExecuteApplyRemoteSubtreeDeleteError::LocalApplyFailed { .. },
                ) => "recursive_remove_io",
                other => panic!("unexpected subtree delete execution error: {other:?}"),
            };
            assert_eq!(actual_kind, execute_error_kind);
        }
    }
}

fn assert_subtree_states_match(
    db: &SqliteStateDb,
    initial: &SubtreeDeleteObservationInitial,
    expect: &SubtreeDeleteObservationExpect,
) {
    let namespace_id = initial
        .remote_state
        .first()
        .map(|row| row.namespace_id.clone())
        .or_else(|| {
            initial
                .local_state
                .first()
                .map(|row| row.namespace_id.clone())
        })
        .expect("subtree fixture namespace");
    let mut inode_ids = BTreeSet::new();
    inode_ids.extend(initial.remote_state.iter().map(|row| row.inode_id));
    inode_ids.extend(initial.local_state.iter().map(|row| row.inode_id));
    inode_ids.extend(initial.sync_anchor.iter().map(|row| row.inode_id));

    let expected_remote = expect
        .remote_state_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_local = expect
        .local_state_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_anchor = expect
        .sync_anchor_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_planned = expect
        .planned_actions_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();

    for inode_id in inode_ids {
        let views = db
            .load_file_sync_views(&namespace_id, inode_id)
            .expect("load subtree views");
        assert_eq!(views.remote, expected_remote.get(&inode_id).cloned());
        assert_eq!(views.local, expected_local.get(&inode_id).cloned());
        assert_eq!(views.sync_anchor, expected_anchor.get(&inode_id).cloned());
        assert_eq!(
            db.load_planned_action(&namespace_id, inode_id)
                .expect("load subtree planned action"),
            expected_planned.get(&inode_id).cloned()
        );
    }
}

fn seed_remote_subtree_rename_observation_state(
    db_path: &Path,
    initial: &SubtreeRenameObservationInitial,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-subtree-rename-observation-state", |tx| {
        for row in &initial.remote_state {
            tx.upsert_remote_file(row)?;
        }
        for row in &initial.local_state {
            tx.upsert_local_file(row)?;
        }
        for row in &initial.sync_anchor {
            tx.upsert_sync_anchor(row)?;
        }
        for row in &initial.planned_actions {
            tx.upsert_planned_action(row)?;
        }
        for row in &initial.pending_inode_mutations {
            let row = row.clone().into_row();
            tx.record_pending_inode_mutation(
                &row.namespace_id,
                row.inode_id,
                &row.request,
                row.created_at_ms,
            )?;
        }
        for row in &initial.transfer_ledgers {
            tx.upsert_transfer_ledger(&row.clone().into_row())?;
        }
        Ok(())
    })
    .expect("seed remote subtree rename observation state");
}

fn assert_subtree_rename_execution_matches_expectation(
    result: Result<Option<NextClientAction>, ExecuteNextClientActionError>,
    expected: &RawSubtreeRenameExecutionExpect,
) {
    match expected {
        RawSubtreeRenameExecutionExpect::ExecutedApplyRemoteSubtreeRename {
            executed_apply_remote_subtree_rename,
        } => {
            let executed = result.expect("subtree rename execution should succeed");
            assert_eq!(
                executed,
                Some(NextClientAction::ExecutedApplyRemoteSubtreeRename(
                    ExecutedApplyRemoteSubtreeRename {
                        applied: executed_apply_remote_subtree_rename.clone(),
                    },
                ))
            );
        }
        RawSubtreeRenameExecutionExpect::ExecuteErrorKind { execute_error_kind } => {
            let error = result.expect_err("subtree rename execution should fail");
            let actual_kind = match error {
                ExecuteNextClientActionError::ApplyRemoteSubtreeRename(
                    ExecuteApplyRemoteSubtreeRenameError::CurrentPathMissing { .. },
                ) => "current_path_missing",
                ExecuteNextClientActionError::ApplyRemoteSubtreeRename(
                    ExecuteApplyRemoteSubtreeRenameError::TargetPathMissing { .. },
                ) => "target_path_missing",
                ExecuteNextClientActionError::ApplyRemoteSubtreeRename(
                    ExecuteApplyRemoteSubtreeRenameError::DestinationOccupied { .. },
                ) => "destination_occupied",
                ExecuteNextClientActionError::ApplyRemoteSubtreeRename(
                    ExecuteApplyRemoteSubtreeRenameError::LocalApplyFailed { .. },
                ) => "rename_io",
                other => panic!("unexpected subtree rename execution error: {other:?}"),
            };
            assert_eq!(actual_kind, execute_error_kind);
        }
    }
}

fn assert_subtree_rename_states_match(
    db: &SqliteStateDb,
    initial: &SubtreeRenameObservationInitial,
    expect: &SubtreeRenameObservationExpect,
) {
    let namespace_id = initial
        .remote_state
        .first()
        .map(|row| row.namespace_id.clone())
        .or_else(|| {
            initial
                .local_state
                .first()
                .map(|row| row.namespace_id.clone())
        })
        .expect("subtree rename fixture namespace");
    let mut inode_ids = BTreeSet::new();
    inode_ids.extend(initial.remote_state.iter().map(|row| row.inode_id));
    inode_ids.extend(initial.local_state.iter().map(|row| row.inode_id));
    inode_ids.extend(initial.sync_anchor.iter().map(|row| row.inode_id));

    let expected_remote = expect
        .remote_state_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_local = expect
        .local_state_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_anchor = expect
        .sync_anchor_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();
    let expected_planned = expect
        .planned_actions_after
        .iter()
        .cloned()
        .map(|row| (row.inode_id, row))
        .collect::<BTreeMap<_, _>>();

    for inode_id in inode_ids {
        let views = db
            .load_file_sync_views(&namespace_id, inode_id)
            .expect("load subtree rename views");
        assert_eq!(views.remote, expected_remote.get(&inode_id).cloned());
        assert_eq!(views.local, expected_local.get(&inode_id).cloned());
        assert_eq!(views.sync_anchor, expected_anchor.get(&inode_id).cloned());
        assert_eq!(
            db.load_planned_action(&namespace_id, inode_id)
                .expect("load subtree rename planned action"),
            expected_planned.get(&inode_id).cloned()
        );
    }
}

fn remove_local_path_if_present(path: &Path) {
    if !path.exists() {
        return;
    }
    if path.is_dir() {
        fs::remove_dir_all(path).expect("remove existing directory path");
    } else {
        fs::remove_file(path).expect("remove existing file path");
    }
}

fn rooted_subtree_inode_ids(
    local_rows: &[LocalFileStateRow],
    root_inode_id: InodeId,
) -> BTreeSet<InodeId> {
    let mut subtree_inode_ids = BTreeSet::from([root_inode_id]);
    loop {
        let mut changed = false;
        for row in local_rows {
            if subtree_inode_ids.contains(&row.inode_id) {
                continue;
            }
            if row
                .parent_inode_id
                .is_some_and(|parent_inode_id| subtree_inode_ids.contains(&parent_inode_id))
            {
                changed = subtree_inode_ids.insert(row.inode_id) || changed;
            }
        }
        if !changed {
            break;
        }
    }
    subtree_inode_ids
}

fn seed_remote_rename_observation_state(db_path: &Path, initial: &RenameObservationInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-remote-rename-observation-state", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        Ok(())
    })
    .expect("seed remote rename observation state");
}

fn combine_reconciliation_reports<I>(reports: I) -> ClientReconciliationInvariantReport
where
    I: IntoIterator<Item = ClientReconciliationInvariantReport>,
{
    let checks = reports
        .into_iter()
        .flat_map(|report| report.checks)
        .collect::<Vec<_>>();
    ClientReconciliationInvariantReport { checks }
}

fn assert_rename_execution_matches_expectation(
    result: Result<Option<NextClientAction>, ExecuteNextClientActionError>,
    expected: &RawRenameExecutionExpect,
) {
    match expected {
        RawRenameExecutionExpect::ExecutedApplyRemoteRename {
            executed_apply_remote_rename,
        } => {
            let executed = result.expect("rename execution should succeed");
            assert_eq!(
                executed,
                Some(NextClientAction::ExecutedApplyRemoteRename(
                    ExecutedApplyRemoteRename {
                        applied: executed_apply_remote_rename.clone(),
                    },
                ))
            );
        }
        RawRenameExecutionExpect::ExecuteErrorKind { execute_error_kind } => {
            let error = result.expect_err("rename execution should fail");
            let actual_kind = match error {
                ExecuteNextClientActionError::ApplyRemoteRename(
                    ExecuteApplyRemoteRenameError::CurrentPathMissing { .. },
                ) => "current_path_missing",
                ExecuteNextClientActionError::ApplyRemoteRename(
                    ExecuteApplyRemoteRenameError::TargetPathMissing { .. },
                ) => "target_path_missing",
                ExecuteNextClientActionError::ApplyRemoteRename(
                    ExecuteApplyRemoteRenameError::DestinationOccupied { .. },
                ) => "destination_occupied",
                ExecuteNextClientActionError::ApplyRemoteRename(
                    ExecuteApplyRemoteRenameError::LocalApplyFailed { .. },
                ) => "rename_io",
                other => panic!("unexpected rename execution error: {other:?}"),
            };
            assert_eq!(actual_kind, execute_error_kind);
        }
    }
}

fn assert_delete_execution_matches_expectation(
    result: Result<Option<NextClientAction>, ExecuteNextClientActionError>,
    expected: &RawDeleteExecutionExpect,
) {
    match expected {
        RawDeleteExecutionExpect::ExecutedApplyRemoteDelete {
            executed_apply_remote_delete,
        } => {
            let executed = result.expect("delete execution should succeed");
            assert_eq!(
                executed,
                Some(NextClientAction::ExecutedApplyRemoteDelete(
                    ExecutedApplyRemoteDelete {
                        applied: executed_apply_remote_delete.clone(),
                    },
                ))
            );
        }
        RawDeleteExecutionExpect::ExecuteErrorKind { execute_error_kind } => {
            let error = result.expect_err("delete execution should fail");
            let actual_kind = match error {
                ExecuteNextClientActionError::ApplyRemoteDelete(
                    ExecuteApplyRemoteDeleteError::CurrentPathMissing { .. },
                ) => "current_path_missing",
                ExecuteNextClientActionError::ApplyRemoteDelete(
                    ExecuteApplyRemoteDeleteError::LocalApplyFailed { .. },
                ) => "unlink_io",
                other => panic!("unexpected delete execution error: {other:?}"),
            };
            assert_eq!(actual_kind, execute_error_kind);
        }
    }
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

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Deserialize)]
struct CreateObservationInitial {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    pending_client_mutation: RawPendingClientMutationRow,
}

#[derive(Debug, Deserialize)]
struct CreateObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    local_only_upload_cleared: bool,
    planned_local_only_action_cleared: bool,
    pending_client_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EditObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    planned_action: PlannedActionRow,
    pending_inode_mutation: RawPendingInodeMutationRow,
    #[serde(default)]
    transfer_ledger: Option<FixtureTransferLedgerSeed>,
}

#[derive(Debug, Deserialize)]
struct EditObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    pending_inode_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DownloadObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    planned_action: PlannedActionRow,
    transfer_ledger: FixtureTransferLedgerSeed,
}

#[derive(Debug, Deserialize)]
struct DownloadObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    planned_action_cleared: bool,
    planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BoundDownloadObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    planned_action: PlannedActionRow,
    transfer_ledger: FixtureTransferLedgerSeed,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BoundDownloadObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DeleteObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    #[serde(default = "default_true")]
    seed_local_file: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: Option<LocalFileStateRow>,
    sync_anchor: Option<SyncAnchorRow>,
    planned_action_cleared: bool,
    #[serde(default)]
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    execution: RawDeleteExecutionExpect,
    first_planner_result: PlannedActionRecord,
    second_planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DeleteConflictObservationExpect {
    outcome: RawAppliedRemoteObservation,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct SubtreeDeleteObservationInitial {
    root_relative_path: PathBuf,
    remote_state: Vec<RemoteFileStateRow>,
    local_state: Vec<LocalFileStateRow>,
    sync_anchor: Vec<SyncAnchorRow>,
    local_entries: Vec<FixtureLocalTreeEntry>,
    #[serde(default)]
    planned_actions: Vec<PlannedActionRow>,
    #[serde(default)]
    pending_inode_mutations: Vec<RawPendingInodeMutationRow>,
    #[serde(default)]
    transfer_ledgers: Vec<FixtureBoundTransferLedgerSeed>,
}

#[derive(Debug, Deserialize)]
struct SubtreeDeleteObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state_after: Vec<RemoteFileStateRow>,
    #[serde(default)]
    local_state_after: Vec<LocalFileStateRow>,
    #[serde(default)]
    sync_anchor_after: Vec<SyncAnchorRow>,
    #[serde(default)]
    planned_actions_after: Vec<PlannedActionRow>,
    #[serde(default)]
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    execution: RawSubtreeDeleteExecutionExpect,
    first_planner_result: PlannedActionRecord,
    second_planner_result: PlannedActionRecord,
    path_expectations: Vec<FixturePathExpectation>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SubtreeDeleteConflictObservationExpect {
    outcome: RawAppliedRemoteObservation,
    planner_result: PlannedActionRecord,
    path_expectations: Vec<FixturePathExpectation>,
}

#[derive(Debug, Deserialize)]
struct SubtreeRenameObservationInitial {
    root_relative_path: PathBuf,
    target_relative_path: PathBuf,
    remote_state: Vec<RemoteFileStateRow>,
    local_state: Vec<LocalFileStateRow>,
    sync_anchor: Vec<SyncAnchorRow>,
    local_entries: Vec<FixtureLocalTreeEntry>,
    #[serde(default)]
    planned_actions: Vec<PlannedActionRow>,
    #[serde(default)]
    pending_inode_mutations: Vec<RawPendingInodeMutationRow>,
    #[serde(default)]
    transfer_ledgers: Vec<FixtureBoundTransferLedgerSeed>,
}

#[derive(Debug, Deserialize)]
struct SubtreeRenameObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state_after: Vec<RemoteFileStateRow>,
    #[serde(default)]
    local_state_after: Vec<LocalFileStateRow>,
    #[serde(default)]
    sync_anchor_after: Vec<SyncAnchorRow>,
    #[serde(default)]
    planned_actions_after: Vec<PlannedActionRow>,
    #[serde(default)]
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    execution: RawSubtreeRenameExecutionExpect,
    first_planner_result: PlannedActionRecord,
    second_planner_result: PlannedActionRecord,
    path_expectations: Vec<FixturePathExpectation>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SubtreeRenameConflictObservationExpect {
    outcome: RawAppliedRemoteObservation,
    planner_result: PlannedActionRecord,
    path_expectations: Vec<FixturePathExpectation>,
}

#[derive(Debug, Deserialize)]
struct RenameObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    target_relative_path: PathBuf,
    #[serde(default)]
    occupied_target_content_utf8: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RenameObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    #[serde(default)]
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    execution: RawRenameExecutionExpect,
    first_planner_result: PlannedActionRecord,
    second_planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
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
struct AmbiguousObservationInitial {
    local_only_state: Vec<LocalOnlyFileStateRow>,
}

#[derive(Debug, Deserialize)]
struct AmbiguousObservationExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: Option<RemoteFileStateRow>,
    local_state: Option<LocalFileStateRow>,
    sync_anchor: Option<SyncAnchorRow>,
    conflicts_and_errors: Vec<RawConflictOrErrorExpect>,
    local_only_state_count: usize,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureLocalTreeEntry {
    relative_path: PathBuf,
    inode_kind: InodeKind,
    #[serde(default)]
    content_utf8: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct FixturePathExpectation {
    relative_path: PathBuf,
    exists: bool,
}

#[derive(Debug)]
struct ReconciliationInvariantFixtureRunReport {
    rendered_trace: String,
}

#[derive(Debug)]
struct RenameInvariantFixtureRunReport {
    rendered_trace: String,
    final_current_exists: bool,
    final_target_exists: bool,
}

#[derive(Debug)]
struct DeleteInvariantFixtureRunReport {
    rendered_trace: String,
    final_current_exists: bool,
}

#[derive(Debug)]
struct SubtreeDeleteInvariantFixtureRunReport {
    rendered_trace: String,
    final_root_exists: bool,
}

#[derive(Debug)]
struct SubtreeRenameInvariantFixtureRunReport {
    rendered_trace: String,
    final_root_exists: bool,
    final_target_exists: bool,
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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RemoteRenameFixtureAction {
    ApplyRemoteObservation {
        apply_remote_observation: ApplyRemoteObservationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
    ExecuteNextClientAction {
        execute_next_client_action: ExecuteNextClientActionAction,
    },
}

impl RemoteRenameFixtureAction {
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

    fn execute_next_client_action(&self) -> Option<ExecuteNextClientActionAction> {
        match self {
            Self::ExecuteNextClientAction {
                execute_next_client_action,
            } => Some(execute_next_client_action.clone()),
            _ => None,
        }
    }
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
struct ExecuteNextClientActionAction {
    uploaded_at_ms: u64,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ApplyInodeMutationResponseAction {
    response: ClientMutationResponse,
}

#[derive(Debug, Clone, Deserialize)]
struct ApplyClientMutationResponseAction {
    response: ClientMutationResponse,
}

#[derive(Debug, Clone, Deserialize)]
struct RawPendingClientMutationRow {
    client_request_id: String,
    namespace_id: NamespaceId,
    client_file_id: loon_client::state_db::ClientFileId,
    request: RawClientMutationRequest,
    created_at_ms: u64,
}

impl RawPendingClientMutationRow {
    fn into_row(self) -> PendingClientMutationRow {
        PendingClientMutationRow {
            client_request_id: self.client_request_id,
            namespace_id: self.namespace_id,
            client_file_id: self.client_file_id,
            request: self.request.into_request(),
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawPendingInodeMutationRow {
    client_request_id: String,
    namespace_id: NamespaceId,
    inode_id: InodeId,
    request: RawClientMutationRequest,
    created_at_ms: u64,
}

impl RawPendingInodeMutationRow {
    fn into_row(self) -> PendingInodeMutationRow {
        PendingInodeMutationRow {
            client_request_id: self.client_request_id,
            namespace_id: self.namespace_id,
            inode_id: self.inode_id,
            request: self.request.into_request(),
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: RawClientMutationOp,
}

impl RawClientMutationRequest {
    fn into_request(self) -> ClientMutationRequest {
        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op: match self.op {
                RawClientMutationOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                    parent_inode_id: create_file.parent_inode_id,
                    display_name: create_file.display_name,
                    content_manifest_digest: create_file.content_manifest_digest,
                },
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

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawAppliedRemoteObservation {
    BoundLocalOnly {
        bound_local_only: loon_client::state_db::BoundLocalOnlyFile,
    },
    ConvergedBoundInode {
        converged_bound_inode: loon_client::state_db::AppliedInodeMutation,
    },
    UpdatedBoundRemoteState {
        updated_bound_remote_state: RawAppliedRemoteObservationTarget,
    },
    DiscoveredRemoteOnly {
        discovered_remote_only: RawAppliedRemoteObservationTarget,
    },
    IgnoredStale {
        ignored_stale: RawAppliedRemoteObservationTarget,
    },
    IgnoredUnmatched {
        ignored_unmatched: RawAppliedRemoteObservationTarget,
    },
    RecordedConflictOrError {
        recorded_conflict_or_error: RawRecordedConflictOrError,
    },
}

impl RawAppliedRemoteObservation {
    fn into_outcome(self) -> AppliedRemoteObservation {
        match self {
            Self::BoundLocalOnly { bound_local_only } => {
                AppliedRemoteObservation::BoundLocalOnly(bound_local_only)
            }
            Self::ConvergedBoundInode {
                converged_bound_inode,
            } => AppliedRemoteObservation::ConvergedBoundInode(converged_bound_inode),
            Self::UpdatedBoundRemoteState {
                updated_bound_remote_state,
            } => AppliedRemoteObservation::UpdatedBoundRemoteState {
                namespace_id: updated_bound_remote_state.namespace_id,
                inode_id: updated_bound_remote_state.inode_id,
            },
            Self::DiscoveredRemoteOnly {
                discovered_remote_only,
            } => AppliedRemoteObservation::DiscoveredRemoteOnly {
                namespace_id: discovered_remote_only.namespace_id,
                inode_id: discovered_remote_only.inode_id,
            },
            Self::IgnoredStale { ignored_stale } => AppliedRemoteObservation::IgnoredStale {
                namespace_id: ignored_stale.namespace_id,
                inode_id: ignored_stale.inode_id,
            },
            Self::IgnoredUnmatched { ignored_unmatched } => {
                AppliedRemoteObservation::IgnoredUnmatched {
                    namespace_id: ignored_unmatched.namespace_id,
                    inode_id: ignored_unmatched.inode_id,
                }
            }
            Self::RecordedConflictOrError {
                recorded_conflict_or_error,
            } => AppliedRemoteObservation::RecordedConflictOrError {
                namespace_id: recorded_conflict_or_error.namespace_id,
                inode_id: recorded_conflict_or_error.inode_id,
                kind: recorded_conflict_or_error.kind,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAppliedRemoteObservationTarget {
    namespace_id: NamespaceId,
    inode_id: InodeId,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRecordedConflictOrError {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    kind: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RawConflictOrErrorExpect {
    kind: String,
    summary: String,
    created_at_ms: u64,
    detail_json: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum RawRenameExecutionExpect {
    ExecutedApplyRemoteRename {
        executed_apply_remote_rename: loon_client::state_db::AppliedInodeMutation,
    },
    ExecuteErrorKind {
        execute_error_kind: String,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum RawDeleteExecutionExpect {
    ExecutedApplyRemoteDelete {
        executed_apply_remote_delete: loon_client::state_db::AppliedInodeMutation,
    },
    ExecuteErrorKind {
        execute_error_kind: String,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum RawSubtreeDeleteExecutionExpect {
    ExecutedApplyRemoteSubtreeDelete {
        executed_apply_remote_subtree_delete: loon_client::state_db::AppliedInodeMutation,
    },
    ExecuteErrorKind {
        execute_error_kind: String,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum RawSubtreeRenameExecutionExpect {
    ExecutedApplyRemoteSubtreeRename {
        executed_apply_remote_subtree_rename: loon_client::state_db::AppliedInodeMutation,
    },
    ExecuteErrorKind {
        execute_error_kind: String,
    },
}

impl RawConflictOrErrorExpect {
    fn from_row(row: loon_client::state_db::ConflictOrErrorRow) -> Self {
        Self {
            kind: row.kind,
            summary: row.summary,
            created_at_ms: row.created_at_ms,
            detail_json: row.detail_json,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawClientMutationOp {
    CreateFile { create_file: RawCreateFileOp },
    ReplaceFile { replace_file: RawReplaceFileOp },
}

#[derive(Debug, Clone, Deserialize)]
struct RawCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RawReplaceFileOp {
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    content_manifest_digest: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureBoundTransferLedgerSeed {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    transfer_id: String,
    direction: TransferDirection,
    object_key: String,
    block_index: u64,
    block_count: u64,
    state: TransferState,
    updated_at_ms: u64,
}

impl FixtureBoundTransferLedgerSeed {
    fn into_row(self) -> TransferLedgerRow {
        TransferLedgerRow {
            namespace_id: self.namespace_id,
            inode_id: self.inode_id,
            transfer_id: self.transfer_id,
            direction: self.direction,
            object_key: self.object_key,
            block_index: self.block_index,
            block_count: self.block_count,
            state: self.state,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LateResponseObservationInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    planned_action: PlannedActionRow,
    pending_inode_mutation: RawPendingInodeMutationRow,
}

#[derive(Debug, Deserialize)]
struct LateResponseObservationExpect {
    applied_inode_mutation: loon_client::state_db::AppliedInodeMutation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct LateCreateResponseObservationInitial {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    pending_client_mutation: RawPendingClientMutationRow,
}

#[derive(Debug, Deserialize)]
struct LateCreateResponseObservationExpect {
    bound_identity: BoundLocalOnlyFile,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    pending_client_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum LateResponseObservationAction {
    ApplyRemoteObservation {
        apply_remote_observation: ApplyRemoteObservationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    ApplyInodeMutationResponse {
        apply_inode_mutation_response: ApplyInodeMutationResponseAction,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl LateResponseObservationAction {
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

    fn apply_inode_mutation_response(&self) -> Option<ApplyInodeMutationResponseAction> {
        match self {
            Self::ApplyInodeMutationResponse {
                apply_inode_mutation_response,
            } => Some(apply_inode_mutation_response.clone()),
            _ => None,
        }
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
enum LateCreateResponseObservationAction {
    ApplyRemoteObservation {
        apply_remote_observation: ApplyRemoteObservationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    ApplyClientMutationResponse {
        apply_client_mutation_response: ApplyClientMutationResponseAction,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl LateCreateResponseObservationAction {
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

    fn apply_client_mutation_response(&self) -> Option<ApplyClientMutationResponseAction> {
        match self {
            Self::ApplyClientMutationResponse {
                apply_client_mutation_response,
            } => Some(apply_client_mutation_response.clone()),
            _ => None,
        }
    }

    fn planner(&self) -> Option<PlannerTickAction> {
        match self {
            Self::PlannerTick { planner_tick } => Some(planner_tick.clone()),
            _ => None,
        }
    }
}

type TestDir = loon_testkit::tempdir::TestDir;

fn default_true() -> bool {
    true
}
