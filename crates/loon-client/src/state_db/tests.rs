use super::{
    BoundLocalOnlyFile, ClientFileId, FileSyncViews, LocalFileStateRow,
    LocalOnlyConflictOrErrorRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    LocalOnlyTransferLedgerRow, LocalOnlyUploadRow, ObservedLocalOnlyInode,
    PendingClientMutationRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb, StateDbError,
    SyncAnchorRow, TransferDirection, TransferLedgerRow, TransferState, SCHEMA_VERSION,
};
use crate::upload::UploadedContent;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse,
    ContentManifestEnvelope, ContentManifestPayload, CreatedRemoteInode, InodeId, InodeKind,
    NamespaceId, ReplacedRemoteFile, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
};
use serde_json::json;

#[test]
fn sqlite_state_db_applies_schema_v13() {
    let db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    assert_eq!(
        db.schema_version().expect("read schema version"),
        SCHEMA_VERSION
    );

    for table in [
        "remote_state",
        "local_state",
        "sync_anchor",
        "planned_actions",
        "client_metadata",
        "local_only_state",
        "planned_local_only_actions",
        "local_only_uploads",
        "local_only_transfer_ledger",
        "local_only_conflicts_and_errors",
        "inode_uploads",
        "pending_client_mutations",
        "pending_inode_mutations",
        "transfer_ledger",
        "conflicts_and_errors",
    ] {
        let exists: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .expect("query sqlite_master");
        assert_eq!(exists, 1, "expected table {table} to exist");
    }
}

#[test]
fn sqlite_state_db_migrates_every_historical_schema_version_to_latest() {
    for version in 1..SCHEMA_VERSION {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open historical schema DB");
        super::schema::initialize_connection(&conn).expect("initialize historical schema DB");
        super::schema::install_schema_version_for_test(&mut conn, version)
            .unwrap_or_else(|err| panic!("install schema version {version}: {err}"));

        let mut db = SqliteStateDb { conn };
        db.apply_migrations()
            .unwrap_or_else(|err| panic!("migrate schema version {version}: {err}"));

        assert_eq!(
            db.schema_version().expect("read migrated schema version"),
            SCHEMA_VERSION,
            "expected historical schema version {version} to migrate to latest"
        );

        let mut stmt = db
            .conn
            .prepare("PRAGMA foreign_key_check")
            .expect("prepare foreign_key_check");
        let mut rows = stmt.query([]).expect("run foreign_key_check");
        assert!(
            rows.next()
                .expect("advance foreign_key_check rows")
                .is_none(),
            "expected migrated schema version {version} to have no foreign-key violations"
        );
    }
}

#[test]
fn sqlite_state_db_rejects_invalid_enum_like_values() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    db.planner_transaction("seed-enum-check-parents", |tx| {
        tx.upsert_local_file(&sample_local())?;
        tx.upsert_local_only_file(&sample_local_only())?;
        Ok(())
    })
    .expect("seed rows for enum-like checks");

    assert!(
        db.conn
            .execute(
                "INSERT INTO remote_state (
                    namespace_id,
                    inode_id,
                    observed_seq,
                    revision_no,
                    content_digest,
                    content_manifest_digest,
                    parent_inode_id,
                    display_name,
                    is_deleted,
                    inode_kind
                ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, 0, 'bogus_kind')",
                rusqlite::params![namespace_id.as_str(), 600_u64, 1_u64, 1_u64, "bad.txt"],
            )
            .is_err(),
        "expected inode_kind CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO planned_actions (
                    namespace_id,
                    inode_id,
                    decision,
                    reason,
                    created_at_ms
                ) VALUES (?1, ?2, 'bogus_decision', 'local_differs_from_anchor', ?3)",
                rusqlite::params![namespace_id.as_str(), inode_id.0, 1_700_000_000_000_u64],
            )
            .is_err(),
        "expected planner decision CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO planned_local_only_actions (
                    client_file_id,
                    namespace_id,
                    decision,
                    reason,
                    created_at_ms
                ) VALUES (?1, ?2, 'upload_local_create', 'bogus_reason', ?3)",
                rusqlite::params![
                    client_file_id.as_str(),
                    namespace_id.as_str(),
                    1_700_000_000_000_u64
                ],
            )
            .is_err(),
        "expected planner reason CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO transfer_ledger (
                    namespace_id,
                    inode_id,
                    transfer_id,
                    direction,
                    object_key,
                    block_index,
                    block_count,
                    state,
                    updated_at_ms
                ) VALUES (?1, ?2, 'transfer-1', 'bogus_direction', 'objects/x', 0, 1, 'staging', ?3)",
                rusqlite::params![namespace_id.as_str(), inode_id.0, 1_700_000_000_000_u64],
            )
            .is_err(),
        "expected transfer direction CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO transfer_ledger (
                    namespace_id,
                    inode_id,
                    transfer_id,
                    direction,
                    object_key,
                    block_index,
                    block_count,
                    state,
                    updated_at_ms
                ) VALUES (?1, ?2, 'transfer-2', 'upload', 'objects/x', 0, 1, 'bogus_state', ?3)",
                rusqlite::params![namespace_id.as_str(), inode_id.0, 1_700_000_000_000_u64],
            )
            .is_err(),
        "expected transfer state CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO conflicts_and_errors (
                    namespace_id,
                    inode_id,
                    kind,
                    summary,
                    detail_json,
                    created_at_ms
                ) VALUES (?1, ?2, 'bogus_issue_kind', 'bad issue', '{}', ?3)",
                rusqlite::params![namespace_id.as_str(), inode_id.0, 1_700_000_000_000_u64],
            )
            .is_err(),
        "expected inode issue CHECK constraint to reject invalid value"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO local_only_conflicts_and_errors (
                    client_file_id,
                    namespace_id,
                    kind,
                    summary,
                    detail_json,
                    created_at_ms
                ) VALUES (?1, ?2, 'bogus_local_issue_kind', 'bad issue', '{}', ?3)",
                rusqlite::params![
                    client_file_id.as_str(),
                    namespace_id.as_str(),
                    1_700_000_000_000_u64
                ],
            )
            .is_err(),
        "expected local-only issue CHECK constraint to reject invalid value"
    );
}

#[test]
fn sqlite_state_db_rejects_orphan_adjunct_rows_via_foreign_keys() {
    let db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    assert!(
        db.conn
            .execute(
                "INSERT INTO planned_actions (
                    namespace_id,
                    inode_id,
                    decision,
                    reason,
                    created_at_ms
                ) VALUES ('ns-1', 42, 'download_remote_edit', 'remote_differs_from_anchor', 1)",
                [],
            )
            .is_err(),
        "expected planned_actions FK to reject orphan inode row"
    );

    assert!(
        db.conn
            .execute(
                "INSERT INTO local_only_uploads (
                    client_file_id,
                    namespace_id,
                    file_digest_sha256,
                    content_manifest_digest,
                    manifest_object_key,
                    file_size_bytes,
                    uploaded_at_ms
                ) VALUES (
                    'tmp:ns-1:00000000000000000001',
                    'ns-1',
                    'sha256:file',
                    'sha256:manifest',
                    'namespaces/ns-1/manifests/sha256:manifest.json',
                    1,
                    1
                )",
                [],
            )
            .is_err(),
        "expected local_only_uploads FK to reject orphan temp row"
    );
}

#[test]
fn sqlite_state_db_creates_explicit_active_read_indexes() {
    let db = SqliteStateDb::open_in_memory().expect("open in-memory DB");

    for index in [
        "idx_planned_actions_created_at",
        "idx_planned_local_only_actions_created_at",
        "idx_transfer_ledger_inode_direction",
        "idx_local_only_transfer_ledger_client_direction",
        "idx_conflicts_and_errors_inode_created_at",
        "idx_local_only_conflicts_and_errors_client_created_at",
    ] {
        let exists: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index],
                |row| row.get(0),
            )
            .expect("query sqlite_master for index");
        assert_eq!(exists, 1, "expected index {index} to exist");
    }
}

#[test]
fn record_local_only_conflict_or_error_replaces_previous_row_for_same_client_file_and_kind() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let namespace_id = NamespaceId::from("ns-1");

    seed_local_only_rows(
        &mut db,
        &[sample_local_only_with(
            client_file_id.as_str(),
            InodeKind::File,
        )],
    );

    db.record_local_only_conflict_or_error(
        &client_file_id,
        &namespace_id,
        "upload_local_create_upload_failed",
        "first summary",
        &json!({"failure": "source_path_missing"}),
        10,
    )
    .expect("record first temp issue");
    db.record_local_only_conflict_or_error(
        &client_file_id,
        &namespace_id,
        "upload_local_create_upload_failed",
        "second summary",
        &json!({"failure": "local_file_read"}),
        20,
    )
    .expect("record second temp issue");

    let issues = db
        .load_local_only_conflicts_and_errors(&client_file_id)
        .expect("load temp issues");

    assert_eq!(
        issues,
        vec![LocalOnlyConflictOrErrorRow {
            client_file_id,
            namespace_id,
            record_id: issues[0].record_id,
            kind: "upload_local_create_upload_failed".to_owned(),
            summary: "second summary".to_owned(),
            detail_json: json!({"failure": "local_file_read"}),
            created_at_ms: 20,
        }]
    );
}

#[test]
fn planner_transaction_rolls_back_partial_write() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    let error = db
        .planner_transaction("rollback-test", |tx| {
            tx.upsert_remote_file(&sample_remote())?;
            Err::<(), _>(StateDbError::UnsupportedSchemaVersion(99))
        })
        .expect_err("transaction should fail");

    assert!(matches!(error, StateDbError::UnsupportedSchemaVersion(99)));
    assert_eq!(
        db.load_file_sync_views(&namespace_id, inode_id)
            .expect("load views after rollback"),
        FileSyncViews {
            namespace_id,
            inode_id,
            remote: None,
            local: None,
            sync_anchor: None,
        }
    );
}

#[test]
fn planner_transaction_persists_three_views() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    db.planner_transaction("seed-views", |tx| {
        tx.upsert_remote_file(&sample_remote())?;
        tx.upsert_local_file(&sample_local())?;
        tx.upsert_sync_anchor(&sample_anchor())?;
        Ok(())
    })
    .expect("seed state");

    let views = db
        .load_file_sync_views(&namespace_id, inode_id)
        .expect("load seeded views");
    assert_eq!(views.remote, Some(sample_remote()));
    assert_eq!(views.local, Some(sample_local()));
    assert_eq!(views.sync_anchor, Some(sample_anchor()));
}

#[test]
fn record_conflict_or_error_replaces_previous_row_for_same_inode_and_kind() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    seed_local_rows(&mut db, &[sample_local_with("ns-1", 42, InodeKind::File)]);

    db.record_conflict_or_error(
        &namespace_id,
        inode_id,
        "remote_observation_bind_ambiguous",
        "first summary",
        &json!({"matches": 2}),
        10,
    )
    .expect("record first issue");
    db.record_conflict_or_error(
        &namespace_id,
        inode_id,
        "remote_observation_bind_ambiguous",
        "second summary",
        &json!({"matches": 3}),
        20,
    )
    .expect("record second issue");

    let issues = db
        .load_conflicts_and_errors(&namespace_id, inode_id)
        .expect("load issues");

    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].kind, "remote_observation_bind_ambiguous");
    assert_eq!(issues[0].summary, "second summary");
    assert_eq!(issues[0].detail_json, json!({"matches": 3}));
    assert_eq!(issues[0].created_at_ms, 20);
}

#[test]
fn allocate_local_file_ids_monotonically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let first = db
        .allocate_local_file_id(&NamespaceId::from("ns-1"))
        .expect("allocate first temp id");
    let second = db
        .allocate_local_file_id(&NamespaceId::from("ns-1"))
        .expect("allocate second temp id");

    assert_eq!(first, ClientFileId::from("tmp:ns-1:00000000000000000001"));
    assert_eq!(second, ClientFileId::from("tmp:ns-1:00000000000000000002"));
}

#[test]
fn allocate_client_request_ids_monotonically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let first = db
        .allocate_client_request_id()
        .expect("allocate first request id");
    let second = db
        .allocate_client_request_id()
        .expect("allocate second request id");

    assert_eq!(first, "client-req-00000000000000000001");
    assert_eq!(second, "client-req-00000000000000000002");
}

#[test]
fn transfer_ledger_round_trips_by_inode_and_direction() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_rows(&mut db, &[sample_local_with("ns-1", 601, InodeKind::File)]);

    let row = TransferLedgerRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(601),
        transfer_id: "upload:ns-1:601:sha256:manifest-1".to_owned(),
        direction: TransferDirection::Upload,
        object_key: "namespaces/ns-1/manifests/sha256:manifest-1.json".to_owned(),
        block_index: 1,
        block_count: 2,
        state: TransferState::Uploading,
        updated_at_ms: 1_700_000_611_000,
    };

    db.upsert_transfer_ledger(&row)
        .expect("record transfer ledger row");

    assert_eq!(
        db.load_transfer_ledger_for_inode(
            &NamespaceId::from("ns-1"),
            InodeId(601),
            TransferDirection::Upload,
        )
        .expect("load transfer ledger row"),
        Some(row.clone())
    );

    db.delete_transfer_ledger_for_inode(
        &NamespaceId::from("ns-1"),
        InodeId(601),
        TransferDirection::Upload,
    )
    .expect("delete transfer ledger row");

    assert_eq!(
        db.load_transfer_ledger_for_inode(
            &NamespaceId::from("ns-1"),
            InodeId(601),
            TransferDirection::Upload,
        )
        .expect("load deleted transfer ledger row"),
        None
    );
}

#[test]
fn local_only_transfer_ledger_round_trips_by_client_file_and_direction() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_only_rows(
        &mut db,
        &[sample_local_only_with(
            "tmp:ns-1:00000000000000000001",
            InodeKind::File,
        )],
    );

    let row = LocalOnlyTransferLedgerRow {
        client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
        namespace_id: NamespaceId::from("ns-1"),
        transfer_id: "upload-local-only:tmp:ns-1:00000000000000000001:sha256:manifest-1".to_owned(),
        direction: TransferDirection::Upload,
        object_key: "namespaces/ns-1/manifests/sha256:manifest-1.json".to_owned(),
        block_index: 1,
        block_count: 2,
        state: TransferState::Uploading,
        updated_at_ms: 1_700_000_611_000,
    };

    db.upsert_local_only_transfer_ledger(&row)
        .expect("record local-only transfer ledger row");

    assert_eq!(
        db.load_local_only_transfer_ledger(
            &ClientFileId::from("tmp:ns-1:00000000000000000001"),
            TransferDirection::Upload,
        )
        .expect("load local-only transfer ledger row"),
        Some(row.clone())
    );

    db.delete_local_only_transfer_ledger(
        &ClientFileId::from("tmp:ns-1:00000000000000000001"),
        TransferDirection::Upload,
    )
    .expect("delete local-only transfer ledger row");

    assert_eq!(
        db.load_local_only_transfer_ledger(
            &ClientFileId::from("tmp:ns-1:00000000000000000001"),
            TransferDirection::Upload,
        )
        .expect("load deleted local-only transfer ledger row"),
        None
    );
}

#[test]
fn planner_transaction_persists_local_only_state() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let local_only = sample_local_only();

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    assert_eq!(
        db.load_local_only_file(&local_only.client_file_id)
            .expect("load local-only state"),
        Some(local_only)
    );
}

#[test]
fn load_next_planned_local_only_action_orders_deterministically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_only_rows(
        &mut db,
        &[
            sample_local_only_with("tmp:ns-1:00000000000000000003", InodeKind::File),
            sample_local_only_with("tmp:ns-1:00000000000000000002", InodeKind::Dir),
            sample_local_only_with("tmp:ns-1:00000000000000000001", InodeKind::File),
        ],
    );

    db.planner_transaction("seed-planned-local-only-actions", |tx| {
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000003"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_300_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000002"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "create_remote_dir".to_owned(),
            reason: "local_only_directory_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        Ok(())
    })
    .expect("seed planned local-only actions");

    assert_eq!(
        db.load_next_planned_local_only_action()
            .expect("load next planned local-only action"),
        Some(LocalOnlyPlannedActionRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn load_next_planned_action_orders_deterministically() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_rows(
        &mut db,
        &[
            sample_local_with("ns-2", 9, InodeKind::File),
            sample_local_with("ns-1", 8, InodeKind::File),
            sample_local_with("ns-1", 7, InodeKind::File),
        ],
    );

    db.planner_transaction("seed-planned-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-2"),
            inode_id: InodeId(9),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_300_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_planned_action()
            .expect("load next planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn load_next_executable_planned_action_skips_deferred_rows() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_rows(
        &mut db,
        &[
            sample_local_with("ns-1", 7, InodeKind::File),
            sample_local_with("ns-1", 8, InodeKind::File),
            sample_local_with("ns-1", 9, InodeKind::File),
            sample_local_with("ns-1", 10, InodeKind::File),
            sample_local_with("ns-1", 6, InodeKind::Dir),
        ],
    );

    db.planner_transaction("seed-executable-and-deferred-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "apply_remote_rename".to_owned(),
            reason: "remote_path_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_205_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(9),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_210_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(10),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_215_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_executable_planned_action()
            .expect("load next executable planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })
    );
}

#[test]
fn load_next_deferred_planned_action_skips_executable_rows() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_local_rows(
        &mut db,
        &[
            sample_local_with("ns-1", 8, InodeKind::File),
            sample_local_with("ns-1", 7, InodeKind::File),
            sample_local_with("ns-1", 9, InodeKind::File),
            sample_local_with("ns-1", 6, InodeKind::Dir),
        ],
    );

    db.planner_transaction("seed-executable-and-deferred-actions", |tx| {
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(8),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_205_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(9),
            decision: "upload_local_edit".to_owned(),
            reason: "local_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_210_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(6),
            decision: "materialize_remote_dir".to_owned(),
            reason: "remote_observed_without_anchor".to_owned(),
            created_at_ms: 1_700_000_202_000,
        })?;
        Ok(())
    })
    .expect("seed planned actions");

    assert_eq!(
        db.load_next_deferred_planned_action()
            .expect("load next deferred planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(7),
            decision: "create_conflict_copy".to_owned(),
            reason: "local_and_remote_differ_from_anchor".to_owned(),
            created_at_ms: 1_700_000_200_000,
        })
    );
}

#[test]
fn record_local_only_upload_persists_and_resolves_manifest_digest() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let local_only = sample_local_only();
    let uploaded = sample_uploaded_content();

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    let recorded = db
        .record_local_only_upload(&local_only.client_file_id, &uploaded, 1_700_000_104_000)
        .expect("record local-only upload");

    assert_eq!(
        recorded,
        LocalOnlyUploadRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256: "sha256:new-local-file".to_owned(),
            content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
            manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json"
                .to_owned(),
            file_size_bytes: 15,
            uploaded_at_ms: 1_700_000_104_000,
        }
    );
    assert_eq!(
        db.load_local_only_upload(&local_only.client_file_id)
            .expect("load persisted upload"),
        Some(recorded)
    );
    assert_eq!(
        db.resolve_local_only_upload_content_manifest_digest(&local_only)
            .expect("resolve content manifest digest"),
        "sha256:manifest-new-local-file"
    );
}

#[test]
fn record_local_only_upload_clears_local_only_transfer_ledger() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let local_only = sample_local_only();
    let uploaded = sample_uploaded_content();

    db.planner_transaction("seed-local-only-and-transfer", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        tx.upsert_local_only_transfer_ledger(&LocalOnlyTransferLedgerRow {
            client_file_id: local_only.client_file_id.clone(),
            namespace_id: local_only.namespace_id.clone(),
            transfer_id:
                "upload-local-only:tmp:ns-1:00000000000000000001:sha256:manifest-new-local-file"
                    .to_owned(),
            direction: TransferDirection::Upload,
            object_key: uploaded.manifest_object_key.clone(),
            block_index: 1,
            block_count: 2,
            state: TransferState::Uploading,
            updated_at_ms: 1_700_000_104_000,
        })?;
        Ok(())
    })
    .expect("seed local-only state and transfer");

    db.record_local_only_upload(&local_only.client_file_id, &uploaded, 1_700_000_104_001)
        .expect("record local-only upload");

    assert_eq!(
        db.load_local_only_transfer_ledger(&local_only.client_file_id, TransferDirection::Upload)
            .expect("load local-only transfer ledger after upload"),
        None
    );
}

#[test]
fn record_local_only_upload_rejects_mismatched_digest() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let mut local_only = sample_local_only();
    local_only.content_digest = Some("sha256:edited-after-upload".to_owned());

    db.planner_transaction("seed-local-only", |tx| {
        tx.upsert_local_only_file(&local_only)?;
        Ok(())
    })
    .expect("seed local-only state");

    let error = db
        .record_local_only_upload(
            &local_only.client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )
        .expect_err("stale upload should be rejected");

    assert!(matches!(
        error,
        StateDbError::UploadedContentDigestMismatch { .. }
    ));
}

#[test]
fn record_pending_client_mutation_persists_and_reuses_same_mapping() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    seed_local_only_rows(
        &mut db,
        &[sample_local_only_with(
            client_file_id.as_str(),
            InodeKind::File,
        )],
    );

    let recorded = db
        .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)
        .expect("record pending mutation");
    let reused = db
        .record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_999)
        .expect("reuse same pending mutation");

    assert_eq!(
        recorded,
        PendingClientMutationRow {
            client_request_id: "client-req-0001".to_owned(),
            namespace_id: NamespaceId::from("ns-1"),
            client_file_id: client_file_id.clone(),
            request: request.clone(),
            created_at_ms: 1_700_000_106_000,
        }
    );
    assert_eq!(reused, recorded);
    assert_eq!(
        db.load_pending_client_mutation("client-req-0001")
            .expect("load pending mutation"),
        Some(recorded.clone())
    );
    assert_eq!(
        db.load_pending_client_mutation_for_client_file(&client_file_id)
            .expect("load pending mutation by client file"),
        Some(recorded)
    );
}

#[test]
fn record_pending_client_mutation_rejects_conflicting_temp_identity() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();

    seed_local_only_rows(
        &mut db,
        &[
            sample_local_only_with("tmp:ns-1:00000000000000000001", InodeKind::File),
            sample_local_only_with("tmp:ns-1:00000000000000000002", InodeKind::File),
        ],
    );

    db.record_pending_client_mutation(
        &ClientFileId::from("tmp:ns-1:00000000000000000001"),
        &request,
        1_700_000_106_000,
    )
    .expect("record pending mutation");

    let error = db
        .record_pending_client_mutation(
            &ClientFileId::from("tmp:ns-1:00000000000000000002"),
            &request,
            1_700_000_106_001,
        )
        .expect_err("conflicting mapping should fail");

    assert!(matches!(
        error,
        StateDbError::PendingClientMutationConflict { .. }
    ));
}

#[test]
fn record_pending_client_mutation_rejects_conflicting_request_for_same_client_file() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    seed_local_only_rows(
        &mut db,
        &[sample_local_only_with(
            client_file_id.as_str(),
            InodeKind::File,
        )],
    );

    db.record_pending_client_mutation(
        &client_file_id,
        &sample_client_create_file_request(),
        1_700_000_106_000,
    )
    .expect("record first pending mutation");

    let error = db
        .record_pending_client_mutation(
            &client_file_id,
            &ClientMutationRequest {
                namespace_id: NamespaceId::from("ns-1"),
                client_request_id: "client-req-0002".to_owned(),
                op: ClientMutationOp::CreateFile {
                    parent_inode_id: InodeId(2),
                    display_name: "draft.txt".to_owned(),
                    content_manifest_digest: "sha256:different-manifest".to_owned(),
                },
            },
            1_700_000_106_001,
        )
        .expect_err("conflicting request for same client file should fail");

    assert!(matches!(
        error,
        StateDbError::PendingClientMutationClientFileConflict { .. }
    ));
}

#[test]
fn observe_local_only_inode_under_bound_parent_allocates_and_persists_child() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    seed_bound_directory_parent(&mut db, InodeId(902));

    let observed = ObservedLocalOnlyInode {
        namespace_id: NamespaceId::from("ns-1"),
        inode_kind: InodeKind::File,
        parent_inode_id: InodeId(902),
        display_name: "note.txt".to_owned(),
        content_digest: Some("sha256:child-note".to_owned()),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_300_000,
    };

    let persisted = db
        .observe_local_only_inode_under_parent(&observed)
        .expect("observe local-only child");

    assert_eq!(
        persisted,
        LocalOnlyFileStateRow {
            client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: Some(InodeId(902)),
            display_name: "note.txt".to_owned(),
            content_digest: Some("sha256:child-note".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_300_000,
        }
    );
    assert_eq!(
        db.load_local_only_file(&persisted.client_file_id)
            .expect("load persisted child"),
        Some(persisted)
    );
}

#[test]
fn observe_local_only_inode_under_parent_rejects_unbound_parent() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    db.planner_transaction("seed-unbound-parent", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts-renamed".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_200_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(902),
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed unbound parent");

    let error = db
        .observe_local_only_inode_under_parent(&ObservedLocalOnlyInode {
            namespace_id: NamespaceId::from("ns-1"),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(902),
            display_name: "note.txt".to_owned(),
            content_digest: Some("sha256:child-note".to_owned()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 1_700_000_300_000,
        })
        .expect_err("observe should reject unbound parent");

    assert!(matches!(
        error,
        StateDbError::LocalOnlyParentNotBound {
            parent_inode_id: 902,
            ..
        }
    ));
}

#[test]
fn bind_local_only_file_to_remote_migrates_into_inode_keyed_tables() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let remote = sample_bound_remote();

    db.planner_transaction("seed-bindable-local-only", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        Ok(())
    })
    .expect("seed bindable local-only state");

    let bound = db
        .bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect("bind local-only file");

    assert_eq!(
        bound,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
        }
    );
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
            .expect("load bound views"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
            remote: Some(remote.clone()),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:new-local-file".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_100_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(901),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(500),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:manifest-new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
            }),
        }
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state"),
        None
    );
    assert_eq!(
        db.load_planned_local_only_action(&client_file_id)
            .expect("load temp planned action"),
        None
    );
    assert_eq!(
        db.load_local_only_upload(&client_file_id)
            .expect("load temp upload row"),
        None
    );
}

#[test]
fn bind_local_only_file_to_remote_rejects_mismatched_observation_without_partial_write() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let mut remote = sample_bound_remote();
    remote.display_name = "renamed.txt".to_owned();

    db.planner_transaction("seed-mismatched-local-only", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        Ok(())
    })
    .expect("seed local-only state");

    let error = db
        .bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect_err("bind should reject mismatched observation");

    assert!(matches!(
        error,
        StateDbError::BindObservationMismatch {
            field: "display_name",
            ..
        }
    ));
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(901))
            .expect("load views after rejected bind"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(901),
            remote: None,
            local: None,
            sync_anchor: None,
        }
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state after rejection"),
        Some(sample_local_only())
    );
}

#[test]
fn apply_client_mutation_response_binds_and_clears_pending_row() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let response = sample_client_create_file_response();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");

    db.planner_transaction("seed-local-only-and-plan", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        tx.record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)?;
        Ok(())
    })
    .expect("seed local-only state and pending mutation");

    let bound = db
        .apply_client_mutation_response(&response)
        .expect("apply client mutation response");

    assert_eq!(
        bound,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
        }
    );
    assert_eq!(
        db.load_file_sync_views(&NamespaceId::from("ns-1"), InodeId(501))
            .expect("load bound views"),
        FileSyncViews {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            remote: Some(RemoteFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:new-local-file".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_100_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some("sha256:new-local-file".to_owned()),
                content_manifest_digest: Some("sha256:new-local-file".to_owned(),),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
            }),
        }
    );
    assert_eq!(
        db.load_pending_client_mutation("client-req-0001")
            .expect("load pending mutation after bind"),
        None
    );
    assert_eq!(
        db.load_local_only_file(&client_file_id)
            .expect("load temp local-only state after bind"),
        None
    );
    assert_eq!(
        db.load_local_only_upload(&client_file_id)
            .expect("load temp upload row after bind"),
        None
    );
}

#[test]
fn apply_client_mutation_response_is_idempotent_when_bound_state_already_matches() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let response = sample_client_create_file_response();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let remote = RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(501),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some("sha256:new-local-file".to_owned()),
        content_manifest_digest: Some("sha256:new-local-file".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        is_deleted: false,
    };

    db.planner_transaction("seed-local-only-for-late-create-response", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        tx.record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)?;
        Ok(())
    })
    .expect("seed local-only state and pending mutation");

    let bound = db
        .bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect("late bind local-only file");
    assert_eq!(
        bound,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
        }
    );
    assert!(db
        .load_pending_client_mutation("client-req-0001")
        .expect("load pending mutation after late bind")
        .is_some());

    let applied = db
        .apply_client_mutation_response(&response)
        .expect("matching late create response should be idempotent");

    assert_eq!(
        applied,
        BoundLocalOnlyFile {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
        }
    );
    assert_eq!(
        db.load_pending_client_mutation("client-req-0001")
            .expect("load pending mutation after idempotent response"),
        None
    );
}

#[test]
fn apply_client_mutation_response_rejects_idempotent_fallback_when_bound_state_diverges() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let request = sample_client_create_file_request();
    let response = sample_client_create_file_response();
    let client_file_id = ClientFileId::from("tmp:ns-1:00000000000000000001");
    let remote = RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(501),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(42),
        revision_no: RevisionNo(1),
        content_digest: Some("sha256:new-local-file".to_owned()),
        content_manifest_digest: Some("sha256:new-local-file".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        is_deleted: false,
    };

    db.planner_transaction("seed-diverged-bound-state-for-late-create-response", |tx| {
        tx.upsert_local_only_file(&sample_local_only())?;
        tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
            client_file_id: client_file_id.clone(),
            namespace_id: NamespaceId::from("ns-1"),
            decision: "upload_local_create".to_owned(),
            reason: "local_only_file_without_remote_identity".to_owned(),
            created_at_ms: 1_700_000_105_000,
        })?;
        tx.record_local_only_upload(
            &client_file_id,
            &sample_uploaded_content(),
            1_700_000_104_000,
        )?;
        tx.record_pending_client_mutation(&client_file_id, &request, 1_700_000_106_000)?;
        Ok(())
    })
    .expect("seed local-only state and pending mutation");

    db.bind_local_only_file_to_remote(&client_file_id, &remote)
        .expect("late bind local-only file");
    db.planner_transaction("diverge-bound-local-state", |tx| {
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:diverged-local".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "draft.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_108_000,
        })?;
        Ok(())
    })
    .expect("diverge bound local state");

    let error = db
        .apply_client_mutation_response(&response)
        .expect_err("diverged late create response should fail closed");

    assert!(matches!(error, StateDbError::LocalOnlyFileMissing { .. }));
    assert!(db
        .load_pending_client_mutation("client-req-0001")
        .expect("load pending mutation after failed fallback")
        .is_some());
}

#[test]
fn apply_remote_rename_updates_local_state_and_anchor_and_clears_issue() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    db.planner_transaction("seed-apply-remote-rename", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(420),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(3)),
            display_name: "report-renamed.txt".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_100_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(419),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            decision: "apply_remote_rename".to_owned(),
            reason: "remote_path_differs_from_anchor".to_owned(),
            created_at_ms: 1_700_000_100_500,
        })?;
        Ok(())
    })
    .expect("seed apply_remote_rename state");
    db.record_conflict_or_error(
        &namespace_id,
        inode_id,
        "apply_remote_rename_local_apply_failed",
        "stale failure",
        &json!({"failure": "destination_occupied"}),
        1_700_000_100_600,
    )
    .expect("record stale rename issue");

    let applied = db
        .apply_remote_rename(&namespace_id, inode_id, 1_700_000_101_000)
        .expect("apply remote rename");

    assert_eq!(
        applied,
        super::AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        }
    );
    assert_eq!(
        db.load_file_sync_views(&namespace_id, inode_id)
            .expect("load renamed views"),
        FileSyncViews {
            namespace_id: namespace_id.clone(),
            inode_id,
            remote: Some(RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(3)),
                display_name: "report-renamed.txt".to_owned(),
                is_deleted: false,
            }),
            local: Some(LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some("sha256:anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(3)),
                display_name: "report-renamed.txt".to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_101_000,
            }),
            sync_anchor: Some(SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                synced_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(3)),
                display_name: "report-renamed.txt".to_owned(),
            }),
        }
    );
    assert_eq!(
        db.load_planned_action(&namespace_id, inode_id)
            .expect("load planned action after apply_remote_rename"),
        None
    );
    assert_eq!(
        db.load_conflicts_and_errors(&namespace_id, inode_id)
            .expect("load rename issues after apply_remote_rename"),
        Vec::new()
    );
}

#[test]
fn apply_remote_delete_preserves_tombstone_and_clears_local_state_and_anchor() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);

    db.planner_transaction("seed-apply-remote-delete", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(420),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: true,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_100_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(419),
            revision_no: RevisionNo(17),
            content_digest: Some("sha256:anchor-17".to_owned()),
            content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            decision: "apply_remote_delete".to_owned(),
            reason: "remote_deleted_from_anchor".to_owned(),
            created_at_ms: 1_700_000_100_500,
        })?;
        tx.record_inode_upload(
            &namespace_id,
            inode_id,
            &UploadedContent {
                namespace_id: namespace_id.clone(),
                file_size_bytes: 9,
                file_digest_sha256: "sha256:anchor-17".to_owned(),
                content_manifest_digest: "sha256:manifest-anchor-17".to_owned(),
                manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-anchor-17.json"
                    .to_owned(),
                manifest_envelope: ContentManifestEnvelope::from_payload(ContentManifestPayload {
                    namespace_id: namespace_id.clone(),
                    file_size_bytes: 9,
                    file_digest_sha256: "sha256:anchor-17".to_owned(),
                    block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
                    blocks: Vec::new(),
                })
                .expect("build apply_remote_delete uploaded content"),
                block_objects: Vec::new(),
            },
            1_700_000_100_550,
        )?;
        Ok(())
    })
    .expect("seed apply_remote_delete state");
    db.record_conflict_or_error(
        &namespace_id,
        inode_id,
        "apply_remote_delete_local_apply_failed",
        "stale failure",
        &json!({"failure": "current_path_missing"}),
        1_700_000_100_600,
    )
    .expect("record stale delete issue");

    let applied = db
        .apply_remote_delete(&namespace_id, inode_id, 1_700_000_101_000)
        .expect("apply remote delete");

    assert_eq!(
        applied,
        super::AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        }
    );
    assert_eq!(
        db.load_file_sync_views(&namespace_id, inode_id)
            .expect("load tombstoned views"),
        FileSyncViews {
            namespace_id: namespace_id.clone(),
            inode_id,
            remote: Some(RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id,
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(420),
                revision_no: RevisionNo(17),
                content_digest: Some("sha256:anchor-17".to_owned()),
                content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: true,
            }),
            local: None,
            sync_anchor: None,
        }
    );
    assert_eq!(
        db.load_planned_action(&namespace_id, inode_id)
            .expect("load planned action after apply_remote_delete"),
        None
    );
    assert_eq!(
        db.load_inode_upload(&namespace_id, inode_id)
            .expect("load inode upload after apply_remote_delete"),
        None
    );
    assert_eq!(
        db.load_conflicts_and_errors(&namespace_id, inode_id)
            .expect("load delete issues after apply_remote_delete"),
        Vec::new()
    );
}

#[test]
fn apply_inode_mutation_response_is_idempotent_when_bound_state_already_matches() {
    let mut db = SqliteStateDb::open_in_memory().expect("open in-memory DB");
    let namespace_id = NamespaceId::from("ns-1");
    let inode_id = InodeId(42);
    let response = ClientMutationResponse {
        namespace_id: namespace_id.clone(),
        client_request_id: "client-req-0002".to_owned(),
        committed_seq: ChangeSeq(42),
        created_inode: None,
        replaced_file: Some(ReplacedRemoteFile {
            inode_id,
            inode_kind: InodeKind::File,
            revision_no: RevisionNo(18),
            content_digest: "sha256:replaced-18".to_owned(),
        }),
    };

    db.planner_transaction("seed-matching-bound-state", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            observed_seq: ChangeSeq(42),
            revision_no: RevisionNo(18),
            content_digest: Some("sha256:replaced-18".to_owned()),
            content_manifest_digest: Some("sha256:manifest-replaced-18".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:replaced-18".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_408_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id,
            inode_kind: InodeKind::File,
            synced_seq: ChangeSeq(42),
            revision_no: RevisionNo(18),
            content_digest: Some("sha256:replaced-18".to_owned()),
            content_manifest_digest: Some("sha256:manifest-replaced-18".to_owned()),
            parent_inode_id: Some(InodeId(2)),
            display_name: "report.txt".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed matching bound state");

    let applied = db
        .apply_inode_mutation_response(&response)
        .expect("matching late response should be idempotent");

    assert_eq!(
        applied,
        super::AppliedInodeMutation {
            namespace_id: namespace_id.clone(),
            inode_id,
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&response.client_request_id)
            .expect("load pending inode mutation after idempotent response"),
        None
    );
}

fn sample_remote() -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(420),
        revision_no: RevisionNo(18),
        content_digest: Some("sha256:remote-18".to_owned()),
        content_manifest_digest: Some("sha256:manifest-remote-18".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
        is_deleted: false,
    }
}

fn sample_local() -> LocalFileStateRow {
    LocalFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        content_digest: Some("sha256:local-edit".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_001_000,
    }
}

fn sample_anchor() -> SyncAnchorRow {
    SyncAnchorRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(42),
        inode_kind: InodeKind::File,
        synced_seq: ChangeSeq(419),
        revision_no: RevisionNo(17),
        content_digest: Some("sha256:anchor-17".to_owned()),
        content_manifest_digest: Some("sha256:manifest-anchor-17".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "report.txt".to_owned(),
    }
}

fn sample_local_only() -> LocalOnlyFileStateRow {
    LocalOnlyFileStateRow {
        client_file_id: ClientFileId::from("tmp:ns-1:00000000000000000001"),
        namespace_id: NamespaceId::from("ns-1"),
        inode_kind: InodeKind::File,
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        content_digest: Some("sha256:new-local-file".to_owned()),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_100_000,
    }
}

fn sample_bound_remote() -> RemoteFileStateRow {
    RemoteFileStateRow {
        namespace_id: NamespaceId::from("ns-1"),
        inode_id: InodeId(901),
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(500),
        revision_no: RevisionNo(1),
        content_digest: Some("sha256:new-local-file".to_owned()),
        content_manifest_digest: Some("sha256:manifest-new-local-file".to_owned()),
        parent_inode_id: Some(InodeId(2)),
        display_name: "draft.txt".to_owned(),
        is_deleted: false,
    }
}

fn sample_client_create_file_request() -> ClientMutationRequest {
    ClientMutationRequest {
        namespace_id: NamespaceId::from("ns-1"),
        client_request_id: "client-req-0001".to_owned(),
        op: ClientMutationOp::CreateFile {
            parent_inode_id: InodeId(2),
            display_name: "draft.txt".to_owned(),
            content_manifest_digest: "sha256:new-local-file".to_owned(),
        },
    }
}

fn sample_client_create_file_response() -> ClientMutationResponse {
    ClientMutationResponse {
        namespace_id: NamespaceId::from("ns-1"),
        client_request_id: "client-req-0001".to_owned(),
        committed_seq: ChangeSeq(42),
        created_inode: Some(CreatedRemoteInode {
            inode_id: InodeId(501),
            inode_kind: InodeKind::File,
            revision_no: RevisionNo(1),
            parent_inode_id: InodeId(2),
            display_name: "draft.txt".to_owned(),
            content_digest: Some("sha256:new-local-file".to_owned()),
        }),
        replaced_file: None,
    }
}

fn sample_uploaded_content() -> UploadedContent {
    UploadedContent {
        namespace_id: NamespaceId::from("ns-1"),
        file_size_bytes: 15,
        file_digest_sha256: "sha256:new-local-file".to_owned(),
        content_manifest_digest: "sha256:manifest-new-local-file".to_owned(),
        manifest_object_key: "namespaces/ns-1/manifests/sha256:manifest-new-local-file.json"
            .to_owned(),
        manifest_envelope: ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: NamespaceId::from("ns-1"),
            file_size_bytes: 15,
            file_digest_sha256: "sha256:new-local-file".to_owned(),
            block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
            blocks: Vec::new(),
        })
        .expect("build sample manifest envelope"),
        block_objects: Vec::new(),
    }
}

fn seed_bound_directory_parent(db: &mut SqliteStateDb, inode_id: InodeId) {
    db.planner_transaction("seed-bound-directory-parent", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            observed_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            is_deleted: false,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_700_000_200_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id,
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(501),
            revision_no: RevisionNo(1),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: Some(InodeId(2)),
            display_name: "drafts".to_owned(),
        })?;
        Ok(())
    })
    .expect("seed bound directory parent");
}

fn seed_local_rows(db: &mut SqliteStateDb, rows: &[LocalFileStateRow]) {
    db.planner_transaction("seed-local-rows", |tx| {
        for row in rows {
            tx.upsert_local_file(row)?;
        }
        Ok(())
    })
    .expect("seed local rows");
}

fn seed_local_only_rows(db: &mut SqliteStateDb, rows: &[LocalOnlyFileStateRow]) {
    db.planner_transaction("seed-local-only-rows", |tx| {
        for row in rows {
            tx.upsert_local_only_file(row)?;
        }
        Ok(())
    })
    .expect("seed local-only rows");
}

fn sample_local_with(
    namespace_id: &str,
    inode_id: u64,
    inode_kind: InodeKind,
) -> LocalFileStateRow {
    LocalFileStateRow {
        namespace_id: NamespaceId::from(namespace_id),
        inode_id: InodeId(inode_id),
        inode_kind,
        content_digest: Some(format!("sha256:local-{namespace_id}-{inode_id}")),
        parent_inode_id: Some(InodeId(2)),
        display_name: format!("inode-{inode_id}"),
        exists_on_disk: true,
        dirty: false,
        last_local_change_ms: 1_700_000_000_000 + inode_id,
    }
}

fn sample_local_only_with(client_file_id: &str, inode_kind: InodeKind) -> LocalOnlyFileStateRow {
    LocalOnlyFileStateRow {
        client_file_id: ClientFileId::from(client_file_id),
        namespace_id: NamespaceId::from("ns-1"),
        inode_kind,
        parent_inode_id: Some(InodeId(2)),
        display_name: client_file_id.to_owned(),
        content_digest: Some(format!("sha256:{client_file_id}")),
        exists_on_disk: true,
        dirty: true,
        last_local_change_ms: 1_700_000_100_000,
    }
}
