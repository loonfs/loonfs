use loon_client::planner::PlannerDecision;
use loon_client::state_db::{
    LocalFileStateRow, ObservedBoundDelete, ObservedBoundInode, ObservedLocalOnlySubtreeInode,
    ObservedLocalOnlySubtreeMove, RemoteFileStateRow, SqliteStateDb, StateDbError,
    SubtreeLocalOnlyParentRef, SubtreeObservationOp, SubtreeObservationOutcome, SyncAnchorRow,
};
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};

#[test]
fn observe_subtree_updates_bound_edit_and_nested_local_only_create() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    seed_bound_file(
        &mut db,
        InodeId(2),
        "hello.txt",
        InodeId(1),
        "sha256:hello-v1",
    );

    let outcomes = db
        .observe_subtree_and_plan(
            &[
                SubtreeObservationOp::ObserveBound {
                    observed: ObservedBoundInode {
                        namespace_id: demo_namespace(),
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                        content_digest: Some("sha256:hello-v2".to_owned()),
                        parent_inode_id: Some(InodeId(1)),
                        display_name: "hello.txt".to_owned(),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "drafts".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::Dir,
                        parent: SubtreeLocalOnlyParentRef::Bound {
                            parent_inode_id: InodeId(1),
                        },
                        display_name: "drafts".to_owned(),
                        content_digest: None,
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "drafts/note.txt".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::File,
                        parent: SubtreeLocalOnlyParentRef::BatchLocalOnly {
                            parent_relative_path: "drafts".to_owned(),
                        },
                        display_name: "note.txt".to_owned(),
                        content_digest: Some("sha256:note-v1".to_owned()),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
            ],
            1_000,
        )
        .expect("observe subtree");

    assert_eq!(outcomes.len(), 3);
    match &outcomes[0] {
        SubtreeObservationOutcome::ObservedBound { planned_action, .. } => {
            assert_eq!(planned_action.decision, PlannerDecision::UploadLocalEdit);
        }
        other => panic!("unexpected first outcome: {other:?}"),
    }

    let root_result = match &outcomes[1] {
        SubtreeObservationOutcome::ObservedLocalOnly { result, .. } => result,
        other => panic!("unexpected second outcome: {other:?}"),
    };
    assert_eq!(
        root_result.planned_action.decision,
        PlannerDecision::CreateRemoteDir
    );

    let child_result = match &outcomes[2] {
        SubtreeObservationOutcome::ObservedLocalOnly { result, .. } => result,
        other => panic!("unexpected third outcome: {other:?}"),
    };
    assert_eq!(child_result.planned_action.decision, PlannerDecision::NoOp);

    let parent_links = db
        .load_local_only_parent_links_for_namespace(&demo_namespace())
        .expect("load parent links");
    let note_link = parent_links
        .iter()
        .find(|row| row.client_file_id == child_result.local_only.client_file_id)
        .expect("note parent link present");
    assert_eq!(
        note_link.parent_client_file_id,
        root_result.local_only.client_file_id
    );
}

#[test]
fn observe_subtree_missing_bound_file_plans_delete_file() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    seed_bound_file(
        &mut db,
        InodeId(2),
        "hello.txt",
        InodeId(1),
        "sha256:hello-v1",
    );

    let outcomes = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::DeleteBound {
                observed: ObservedBoundDelete {
                    namespace_id: demo_namespace(),
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    content_digest: Some("sha256:hello-v1".to_owned()),
                    parent_inode_id: Some(InodeId(1)),
                    display_name: "hello.txt".to_owned(),
                    last_local_change_ms: 1_000,
                },
            }],
            1_000,
        )
        .expect("observe subtree delete file");

    match &outcomes[0] {
        SubtreeObservationOutcome::DeletedBound { planned_action, .. } => {
            assert_eq!(planned_action.decision, PlannerDecision::DeleteFile);
        }
        other => panic!("unexpected delete outcome: {other:?}"),
    }
}

#[test]
fn observe_subtree_missing_bound_directory_plans_delete_subtree() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(3), "docs", Some(InodeId(1)));

    let outcomes = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::DeleteBound {
                observed: ObservedBoundDelete {
                    namespace_id: demo_namespace(),
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::Dir,
                    content_digest: None,
                    parent_inode_id: Some(InodeId(1)),
                    display_name: "docs".to_owned(),
                    last_local_change_ms: 1_000,
                },
            }],
            1_000,
        )
        .expect("observe subtree delete directory");

    match &outcomes[0] {
        SubtreeObservationOutcome::DeletedBound { planned_action, .. } => {
            assert_eq!(planned_action.decision, PlannerDecision::DeleteSubtree);
        }
        other => panic!("unexpected delete outcome: {other:?}"),
    }
}

#[test]
fn observe_subtree_repeat_reuses_nested_local_only_identity() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);

    let first = db
        .observe_subtree_and_plan(&nested_local_only_batch(), 1_000)
        .expect("first subtree batch");
    let second = db
        .observe_subtree_and_plan(&nested_local_only_batch(), 2_000)
        .expect("second subtree batch");

    let first_root_id = observed_local_only_id(&first[0]);
    let first_child_id = observed_local_only_id(&first[1]);
    let second_root_id = observed_local_only_id(&second[0]);
    let second_child_id = observed_local_only_id(&second[1]);

    assert_eq!(first_root_id, second_root_id);
    assert_eq!(first_child_id, second_child_id);
}

#[test]
fn observe_subtree_move_bound_file_plans_rename() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(3), "archive", Some(InodeId(1)));
    seed_bound_file(
        &mut db,
        InodeId(2),
        "hello.txt",
        InodeId(1),
        "sha256:hello-v1",
    );

    let outcomes = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::MoveBound {
                from_relative_path: "hello.txt".to_owned(),
                observed: ObservedBoundInode {
                    namespace_id: demo_namespace(),
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    content_digest: Some("sha256:hello-v1".to_owned()),
                    parent_inode_id: Some(InodeId(3)),
                    display_name: "hello.txt".to_owned(),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: 1_000,
                },
            }],
            1_000,
        )
        .expect("observe subtree bound move");

    match &outcomes[0] {
        SubtreeObservationOutcome::MovedBound {
            from_relative_path,
            planned_action,
            ..
        } => {
            assert_eq!(from_relative_path, "hello.txt");
            assert_eq!(planned_action.decision, PlannerDecision::Rename);
        }
        other => panic!("unexpected move outcome: {other:?}"),
    }
}

#[test]
fn observe_subtree_move_local_only_file_under_bound_parent_preserves_identity() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    seed_bound_directory(&mut db, InodeId(3), "archive", Some(InodeId(1)));

    let created = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::ObserveLocalOnly {
                observed: ObservedLocalOnlySubtreeInode {
                    relative_path: "draft.txt".to_owned(),
                    namespace_id: demo_namespace(),
                    inode_kind: InodeKind::File,
                    parent: SubtreeLocalOnlyParentRef::Bound {
                        parent_inode_id: InodeId(1),
                    },
                    display_name: "draft.txt".to_owned(),
                    content_digest: Some("sha256:draft-v1".to_owned()),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: 1_000,
                },
            }],
            1_000,
        )
        .expect("create local-only file");
    let original_client_file_id = observed_local_only_id(&created[0]);

    let moved = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::MoveLocalOnly {
                observed: ObservedLocalOnlySubtreeMove {
                    from_relative_path: "draft.txt".to_owned(),
                    relative_path: "archive/draft.txt".to_owned(),
                    client_file_id: loon_client::state_db::ClientFileId::new(
                        original_client_file_id.clone(),
                    ),
                    namespace_id: demo_namespace(),
                    inode_kind: InodeKind::File,
                    parent: SubtreeLocalOnlyParentRef::Bound {
                        parent_inode_id: InodeId(3),
                    },
                    display_name: "draft.txt".to_owned(),
                    content_digest: Some("sha256:draft-v1".to_owned()),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: 2_000,
                },
            }],
            2_000,
        )
        .expect("move local-only file under bound parent");

    match &moved[0] {
        SubtreeObservationOutcome::MovedLocalOnly {
            from_relative_path,
            result,
            ..
        } => {
            assert_eq!(from_relative_path, "draft.txt");
            assert_eq!(
                result.local_only.client_file_id.as_str(),
                original_client_file_id
            );
            assert_eq!(
                result.planned_action.decision,
                PlannerDecision::UploadLocalCreate
            );
        }
        other => panic!("unexpected local-only move outcome: {other:?}"),
    }
}

#[test]
fn observe_subtree_move_local_only_file_under_local_only_parent_preserves_identity() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);

    let created = db
        .observe_subtree_and_plan(
            &[
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "drafts".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::Dir,
                        parent: SubtreeLocalOnlyParentRef::Bound {
                            parent_inode_id: InodeId(1),
                        },
                        display_name: "drafts".to_owned(),
                        content_digest: None,
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "archive".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::Dir,
                        parent: SubtreeLocalOnlyParentRef::Bound {
                            parent_inode_id: InodeId(1),
                        },
                        display_name: "archive".to_owned(),
                        content_digest: None,
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "drafts/note.txt".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::File,
                        parent: SubtreeLocalOnlyParentRef::BatchLocalOnly {
                            parent_relative_path: "drafts".to_owned(),
                        },
                        display_name: "note.txt".to_owned(),
                        content_digest: Some("sha256:note-v1".to_owned()),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
            ],
            1_000,
        )
        .expect("create local-only subtree");

    let archive_dir_id = observed_local_only_id(&created[1]);
    let original_client_file_id = observed_local_only_id(&created[2]);

    let moved = db
        .observe_subtree_and_plan(
            &[SubtreeObservationOp::MoveLocalOnly {
                observed: ObservedLocalOnlySubtreeMove {
                    from_relative_path: "drafts/note.txt".to_owned(),
                    relative_path: "archive/note.txt".to_owned(),
                    client_file_id: loon_client::state_db::ClientFileId::new(
                        original_client_file_id.clone(),
                    ),
                    namespace_id: demo_namespace(),
                    inode_kind: InodeKind::File,
                    parent: SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                        parent_client_file_id: loon_client::state_db::ClientFileId::new(
                            archive_dir_id.clone(),
                        ),
                    },
                    display_name: "note.txt".to_owned(),
                    content_digest: Some("sha256:note-v1".to_owned()),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: 2_000,
                },
            }],
            2_000,
        )
        .expect("move local-only file under local-only parent");

    match &moved[0] {
        SubtreeObservationOutcome::MovedLocalOnly {
            from_relative_path,
            result,
            ..
        } => {
            assert_eq!(from_relative_path, "drafts/note.txt");
            assert_eq!(
                result.local_only.client_file_id.as_str(),
                original_client_file_id
            );
            assert_eq!(result.planned_action.decision, PlannerDecision::NoOp);
        }
        other => panic!("unexpected local-only move outcome: {other:?}"),
    }
}

#[test]
fn observe_subtree_batch_rollback_is_atomic() {
    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_bound_root_directory(&mut db);
    let before = db
        .load_namespace_state_summary(&demo_namespace())
        .expect("load before summary");

    let error = db
        .observe_subtree_and_plan(
            &[
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "drafts".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::Dir,
                        parent: SubtreeLocalOnlyParentRef::Bound {
                            parent_inode_id: InodeId(1),
                        },
                        display_name: "drafts".to_owned(),
                        content_digest: None,
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
                SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: "bad/note.txt".to_owned(),
                        namespace_id: demo_namespace(),
                        inode_kind: InodeKind::File,
                        parent: SubtreeLocalOnlyParentRef::BatchLocalOnly {
                            parent_relative_path: "missing-parent".to_owned(),
                        },
                        display_name: "note.txt".to_owned(),
                        content_digest: Some("sha256:note-v1".to_owned()),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: 1_000,
                    },
                },
            ],
            1_000,
        )
        .expect_err("missing batch parent should fail");

    assert!(matches!(
        error,
        StateDbError::SubtreeObservationBatchParentMissing { .. }
    ));

    let after = db
        .load_namespace_state_summary(&demo_namespace())
        .expect("load after summary");
    let parent_links = db
        .load_local_only_parent_links_for_namespace(&demo_namespace())
        .expect("load parent links after failed batch");

    assert_eq!(before, after);
    assert!(parent_links.is_empty());
}

fn nested_local_only_batch() -> Vec<SubtreeObservationOp> {
    vec![
        SubtreeObservationOp::ObserveLocalOnly {
            observed: ObservedLocalOnlySubtreeInode {
                relative_path: "drafts".to_owned(),
                namespace_id: demo_namespace(),
                inode_kind: InodeKind::Dir,
                parent: SubtreeLocalOnlyParentRef::Bound {
                    parent_inode_id: InodeId(1),
                },
                display_name: "drafts".to_owned(),
                content_digest: None,
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            },
        },
        SubtreeObservationOp::ObserveLocalOnly {
            observed: ObservedLocalOnlySubtreeInode {
                relative_path: "drafts/note.txt".to_owned(),
                namespace_id: demo_namespace(),
                inode_kind: InodeKind::File,
                parent: SubtreeLocalOnlyParentRef::BatchLocalOnly {
                    parent_relative_path: "drafts".to_owned(),
                },
                display_name: "note.txt".to_owned(),
                content_digest: Some("sha256:note-v1".to_owned()),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            },
        },
    ]
}

fn observed_local_only_id(outcome: &SubtreeObservationOutcome) -> String {
    match outcome {
        SubtreeObservationOutcome::ObservedLocalOnly { result, .. } => {
            result.local_only.client_file_id.as_str().to_owned()
        }
        other => panic!("expected observed local-only outcome, got {other:?}"),
    }
}

fn seed_bound_root_directory(db: &mut SqliteStateDb) {
    db.planner_transaction("seed-bound-root-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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
    .expect("seed bound root");
}

fn seed_bound_file(
    db: &mut SqliteStateDb,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: InodeId,
    content_digest: &str,
) {
    db.planner_transaction("seed-bound-file", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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

fn seed_bound_directory(
    db: &mut SqliteStateDb,
    inode_id: InodeId,
    display_name: &str,
    parent_inode_id: Option<InodeId>,
) {
    db.planner_transaction("seed-bound-directory", |tx| {
        tx.upsert_remote_file(&RemoteFileStateRow {
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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
            namespace_id: demo_namespace(),
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
