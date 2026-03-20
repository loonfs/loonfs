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
    evaluate_remote_observation_active_download_invariants,
    evaluate_remote_observation_active_upload_invariants,
    evaluate_remote_observation_ambiguous_bind_invariants,
    evaluate_remote_observation_convergence_invariants,
    evaluate_remote_observation_late_bind_invariants, ClientReconciliationInvariantReport,
    RemoteObservationActiveDownloadInvariantInputs, RemoteObservationActiveUploadInvariantInputs,
    RemoteObservationAmbiguousBindInvariantInputs, RemoteObservationConvergenceInvariantInputs,
    RemoteObservationLateBindInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{
    ClientMutationOp, ClientMutationRequest, ClientMutationResponse, InodeId, NamespaceId,
    RevisionNo,
};
use serde::Deserialize;
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

#[derive(Debug)]
struct ReconciliationInvariantFixtureRunReport {
    rendered_trace: String,
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
