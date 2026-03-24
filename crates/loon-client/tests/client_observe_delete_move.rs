use loon_client::planner::{PlannerDecision, PlannerReason};
use loon_client::state_db::{
    ClientFileId, LocalOnlyParentRef, LocalOnlyPlannedActionRow, ObservedBoundInode, SqliteStateDb,
};
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::Deserialize;
use std::fs;

#[test]
fn observe_bound_file_delete_fixture_plans_delete_file() {
    let fixture: ObserveFixture =
        load_fixture("client/observe_bound_file_delete_plans_delete_file.yaml");
    let initial = fixture.initial;
    let action = fixture.observe_delete.expect("delete action");
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let bound = initial.bound_file.expect("bound file");
    let planned = db
        .observe_bound_inode_and_plan(
            &ObservedBoundInode {
                namespace_id: initial.namespace_id.clone(),
                inode_id: bound.inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some(bound.local_content_digest),
                parent_inode_id: Some(bound.parent_inode_id),
                display_name: bound.display_name,
                exists_on_disk: false,
                dirty: true,
                last_local_change_ms: 1_000,
            },
            1_000,
        )
        .expect("observe bound file delete");

    assert_eq!(
        action.kind, "bound_file",
        "fixture mismatch for delete kind"
    );
    assert_eq!(action.relative_path, "hello.txt");
    assert_eq!(
        planned.decision,
        expect.planned_decision.expect("planned decision")
    );
    assert_eq!(
        planned.reason,
        expect.planned_reason.expect("planned reason")
    );
}

#[test]
fn observe_bound_directory_delete_fixture_plans_delete_subtree() {
    let fixture: ObserveFixture =
        load_fixture("client/observe_bound_directory_delete_plans_delete_subtree.yaml");
    let initial = fixture.initial;
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let bound = initial.bound_directory.expect("bound directory");
    let planned = db
        .observe_bound_inode_and_plan(
            &ObservedBoundInode {
                namespace_id: initial.namespace_id.clone(),
                inode_id: bound.inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(bound.parent_inode_id),
                display_name: bound.display_name,
                exists_on_disk: false,
                dirty: true,
                last_local_change_ms: 1_000,
            },
            1_000,
        )
        .expect("observe bound directory delete");

    assert_eq!(
        planned.decision,
        expect.planned_decision.expect("planned decision")
    );
    assert_eq!(
        planned.reason,
        expect.planned_reason.expect("planned reason")
    );
}

#[test]
fn observe_bound_file_move_fixture_plans_rename() {
    let fixture: ObserveFixture = load_fixture("client/observe_bound_file_move_plans_rename.yaml");
    let initial = fixture.initial;
    let action = fixture.observe_move.expect("move action");
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let bound = initial.bound_file.expect("bound file");
    let target_parent = initial
        .bound_parent_directory_2
        .expect("second bound parent")
        .inode_id;
    assert_eq!(action.kind, "bound_file");
    assert_eq!(action.from_relative_path, "hello.txt");
    let target_name = action
        .to_relative_path
        .rsplit('/')
        .next()
        .expect("target display name")
        .to_owned();
    let planned = db
        .observe_bound_inode_and_plan(
            &ObservedBoundInode {
                namespace_id: initial.namespace_id.clone(),
                inode_id: bound.inode_id,
                inode_kind: InodeKind::File,
                content_digest: Some(bound.local_content_digest),
                parent_inode_id: Some(target_parent),
                display_name: target_name,
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            },
            1_000,
        )
        .expect("observe bound file move");

    assert_eq!(
        planned.decision,
        expect.planned_decision.expect("planned decision")
    );
    assert_eq!(
        planned.reason,
        expect.planned_reason.expect("planned reason")
    );
}

#[test]
fn observe_bound_directory_move_fixture_plans_rename() {
    let fixture: ObserveFixture =
        load_fixture("client/observe_bound_directory_move_plans_rename.yaml");
    let initial = fixture.initial;
    let action = fixture.observe_move.expect("move action");
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let bound = initial.bound_directory.expect("bound directory");
    let target_parent = initial
        .bound_parent_directory_2
        .expect("second bound parent")
        .inode_id;
    assert_eq!(action.kind, "bound_directory");
    assert_eq!(action.from_relative_path, "docs");
    let target_name = action
        .to_relative_path
        .rsplit('/')
        .next()
        .expect("target display name")
        .to_owned();
    let planned = db
        .observe_bound_inode_and_plan(
            &ObservedBoundInode {
                namespace_id: initial.namespace_id.clone(),
                inode_id: bound.inode_id,
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: Some(target_parent),
                display_name: target_name,
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            },
            1_000,
        )
        .expect("observe bound directory move");

    assert_eq!(
        planned.decision,
        expect.planned_decision.expect("planned decision")
    );
    assert_eq!(
        planned.reason,
        expect.planned_reason.expect("planned reason")
    );
}

#[test]
fn observe_local_only_file_delete_fixture_clears_temp_state() {
    let fixture: ObserveFixture =
        load_fixture("client/observe_local_only_file_delete_clears_temp_state.yaml");
    let initial = fixture.initial;
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let local_only = initial.local_only_child.expect("local only child");
    let deleted = db
        .observe_local_only_delete(&local_only.client_file_id)
        .expect("observe local-only delete");

    assert_eq!(
        db.load_local_only_file(&local_only.client_file_id)
            .expect("load local only row after delete")
            .is_none(),
        expect
            .local_only_row_removed
            .expect("row removed expectation")
    );
    assert_eq!(
        expect.planned_decision.expect("planned decision"),
        PlannerDecision::NoOp
    );
    assert_eq!(deleted.root_client_file_id, local_only.client_file_id);
}

#[test]
fn observe_local_only_directory_move_fixture_preserves_descendant_identities() {
    let fixture: ObserveFixture = load_fixture(
        "client/observe_local_only_directory_move_preserves_descendant_identities.yaml",
    );
    let initial = fixture.initial;
    let expect = fixture.expect;

    let mut db = SqliteStateDb::open_in_memory().expect("open state db");
    seed_fixture_state(&mut db, &initial);

    let directory = initial.local_only_directory.expect("local only directory");
    let moved = db
        .observe_local_only_move_and_plan(
            &directory.client_file_id,
            &LocalOnlyParentRef::Bound {
                parent_inode_id: initial
                    .bound_parent_directory_2
                    .expect("bound parent directory 2")
                    .inode_id,
            },
            InodeKind::Dir,
            &directory.display_name,
            None,
            true,
            true,
            1_000,
            1_000,
        )
        .expect("observe local-only directory move");

    assert_eq!(
        moved.local_only.client_file_id,
        expect.moved_client_file_id.expect("moved client file id")
    );
    assert_eq!(
        moved.planned_action.decision,
        expect.planned_decision.expect("planned decision")
    );

    let child = initial.local_only_child.expect("local only child");
    assert_eq!(
        db.load_local_only_file(&child.client_file_id)
            .expect("load child after parent move")
            .expect("child should remain present")
            .client_file_id,
        expect
            .descendant_client_file_id
            .expect("descendant client file id")
    );
}

#[derive(Debug, Deserialize)]
struct FixtureInitial {
    namespace_id: NamespaceId,
    #[serde(default)]
    bound_parent_directory: Option<BoundParentDirectorySeed>,
    #[serde(default)]
    bound_parent_directory_2: Option<BoundParentDirectorySeed>,
    #[serde(default)]
    bound_file: Option<BoundFileSeed>,
    #[serde(default)]
    bound_directory: Option<BoundDirectorySeed>,
    #[serde(default)]
    local_only_directory: Option<LocalOnlySeed>,
    #[serde(default)]
    local_only_child: Option<LocalOnlyChildSeed>,
}

#[derive(Debug, Deserialize)]
struct BoundParentDirectorySeed {
    inode_id: InodeId,
    relative_path: String,
    exists_on_disk: bool,
    dirty: bool,
}

#[derive(Debug, Deserialize)]
struct BoundFileSeed {
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
    remote_content_digest: String,
    local_content_digest: String,
    sync_anchor_content_digest: String,
}

#[derive(Debug, Deserialize)]
struct BoundDirectorySeed {
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct LocalOnlySeed {
    client_file_id: ClientFileId,
    parent_inode_id: InodeId,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct LocalOnlyChildSeed {
    client_file_id: ClientFileId,
    #[serde(default)]
    parent_inode_id: Option<InodeId>,
    #[serde(default)]
    parent_client_file_id: Option<ClientFileId>,
    display_name: String,
    #[serde(default)]
    content_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ObserveDeleteAction {
    kind: String,
    relative_path: String,
}

#[derive(Debug, Deserialize)]
struct ObserveMoveAction {
    kind: String,
    from_relative_path: String,
    to_relative_path: String,
}

#[derive(Debug, Deserialize)]
struct ObserveFixture {
    initial: FixtureInitial,
    #[serde(default)]
    observe_delete: Option<ObserveDeleteAction>,
    #[serde(default)]
    observe_move: Option<ObserveMoveAction>,
    expect: FixtureExpect,
}

#[derive(Debug, Deserialize)]
struct FixtureExpect {
    #[serde(default)]
    planned_decision: Option<PlannerDecision>,
    #[serde(default)]
    planned_reason: Option<PlannerReason>,
    #[serde(default)]
    local_only_row_removed: Option<bool>,
    #[serde(default)]
    moved_client_file_id: Option<ClientFileId>,
    #[serde(default)]
    descendant_client_file_id: Option<ClientFileId>,
}

fn seed_fixture_state(db: &mut SqliteStateDb, initial: &FixtureInitial) {
    db.planner_transaction("seed observe delete/move fixture", |tx| {
        if initial.bound_parent_directory.is_none()
            && (initial
                .bound_file
                .as_ref()
                .map(|row| row.parent_inode_id == InodeId(1))
                .unwrap_or(false)
                || initial
                    .bound_directory
                    .as_ref()
                    .map(|row| row.parent_inode_id == InodeId(1))
                    .unwrap_or(false))
        {
            seed_bound_parent(
                tx,
                &initial.namespace_id,
                &BoundParentDirectorySeed {
                    inode_id: InodeId(1),
                    relative_path: String::new(),
                    exists_on_disk: true,
                    dirty: false,
                },
            )?;
        }
        if let Some(parent) = initial.bound_parent_directory.as_ref() {
            seed_bound_parent(tx, &initial.namespace_id, parent)?;
        }
        if let Some(parent) = initial.bound_parent_directory_2.as_ref() {
            seed_bound_parent(tx, &initial.namespace_id, parent)?;
        }
        if let Some(bound) = initial.bound_file.as_ref() {
            seed_bound_file(tx, &initial.namespace_id, bound)?;
        }
        if let Some(bound) = initial.bound_directory.as_ref() {
            seed_bound_directory(tx, &initial.namespace_id, bound)?;
        }
        if let Some(local_only) = initial.local_only_directory.as_ref() {
            tx.upsert_local_only_file(&loon_client::state_db::LocalOnlyFileStateRow {
                client_file_id: local_only.client_file_id.clone(),
                namespace_id: initial.namespace_id.clone(),
                inode_kind: InodeKind::Dir,
                parent_inode_id: Some(local_only.parent_inode_id),
                display_name: local_only.display_name.clone(),
                content_digest: None,
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            })?;
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: local_only.client_file_id.clone(),
                namespace_id: initial.namespace_id.clone(),
                decision: "create_remote_dir".to_owned(),
                reason: "local_only_directory_without_remote_identity".to_owned(),
                created_at_ms: 900,
            })?;
        }
        if let Some(local_only) = initial.local_only_child.as_ref() {
            tx.upsert_local_only_file(&loon_client::state_db::LocalOnlyFileStateRow {
                client_file_id: local_only.client_file_id.clone(),
                namespace_id: initial.namespace_id.clone(),
                inode_kind: InodeKind::File,
                parent_inode_id: local_only.parent_inode_id,
                display_name: local_only.display_name.clone(),
                content_digest: local_only.content_digest.clone(),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: 1_000,
            })?;
            if let Some(parent_client_file_id) = local_only.parent_client_file_id.as_ref() {
                tx.upsert_local_only_parent_link(
                    &local_only.client_file_id,
                    parent_client_file_id,
                )?;
            }
            tx.upsert_planned_local_only_action(&LocalOnlyPlannedActionRow {
                client_file_id: local_only.client_file_id.clone(),
                namespace_id: initial.namespace_id.clone(),
                decision: "upload_local_create".to_owned(),
                reason: "local_only_file_without_remote_identity".to_owned(),
                created_at_ms: 900,
            })?;
        }
        Ok(())
    })
    .expect("seed fixture state");
}

fn seed_bound_parent(
    tx: &mut loon_client::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    parent: &BoundParentDirectorySeed,
) -> Result<(), loon_client::state_db::StateDbError> {
    let (parent_inode_id, display_name) = if parent.relative_path.is_empty() {
        (None, String::new())
    } else {
        (
            Some(InodeId(1)),
            parent.relative_path.rsplit('/').next().unwrap().to_owned(),
        )
    };
    tx.upsert_remote_file(&loon_client::state_db::RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: parent.inode_id,
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id,
        display_name: display_name.clone(),
        is_deleted: false,
    })?;
    tx.upsert_local_file(&loon_client::state_db::LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: parent.inode_id,
        inode_kind: InodeKind::Dir,
        content_digest: None,
        parent_inode_id,
        display_name: display_name.clone(),
        exists_on_disk: parent.exists_on_disk,
        dirty: parent.dirty,
        last_local_change_ms: 0,
    })?;
    tx.upsert_sync_anchor(&loon_client::state_db::SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: parent.inode_id,
        inode_kind: InodeKind::Dir,
        synced_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id,
        display_name,
    })?;
    Ok(())
}

fn seed_bound_file(
    tx: &mut loon_client::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    bound: &BoundFileSeed,
) -> Result<(), loon_client::state_db::StateDbError> {
    let manifest_digest = format!("manifest:{}", bound.remote_content_digest);
    tx.upsert_remote_file(&loon_client::state_db::RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::File,
        observed_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: Some(bound.remote_content_digest.clone()),
        content_manifest_digest: Some(manifest_digest.clone()),
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
        is_deleted: false,
    })?;
    tx.upsert_local_file(&loon_client::state_db::LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::File,
        content_digest: Some(bound.local_content_digest.clone()),
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
        exists_on_disk: true,
        dirty: false,
        last_local_change_ms: 0,
    })?;
    tx.upsert_sync_anchor(&loon_client::state_db::SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::File,
        synced_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: Some(bound.sync_anchor_content_digest.clone()),
        content_manifest_digest: Some(format!("manifest:{}", bound.sync_anchor_content_digest)),
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
    })?;
    Ok(())
}

fn seed_bound_directory(
    tx: &mut loon_client::state_db::PlannerTxn<'_>,
    namespace_id: &NamespaceId,
    bound: &BoundDirectorySeed,
) -> Result<(), loon_client::state_db::StateDbError> {
    tx.upsert_remote_file(&loon_client::state_db::RemoteFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::Dir,
        observed_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
        is_deleted: false,
    })?;
    tx.upsert_local_file(&loon_client::state_db::LocalFileStateRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::Dir,
        content_digest: None,
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
        exists_on_disk: true,
        dirty: false,
        last_local_change_ms: 0,
    })?;
    tx.upsert_sync_anchor(&loon_client::state_db::SyncAnchorRow {
        namespace_id: namespace_id.clone(),
        inode_id: bound.inode_id,
        inode_kind: InodeKind::Dir,
        synced_seq: ChangeSeq(1),
        revision_no: RevisionNo(1),
        content_digest: None,
        content_manifest_digest: None,
        parent_inode_id: Some(bound.parent_inode_id),
        display_name: bound.display_name.clone(),
    })?;
    Ok(())
}

fn load_fixture<T>(relative_path: &str) -> T
where
    T: for<'de> Deserialize<'de>,
{
    let path = loon_testkit::fixtures::fixture_path(relative_path);
    let text = fs::read_to_string(&path).expect("read observe fixture");
    serde_yaml::from_str(&text).expect("decode observe fixture")
}
