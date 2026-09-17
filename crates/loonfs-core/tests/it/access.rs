//! Access updates, structural rules, and revision continuity after a flush.

use crate::common::commit_split_support::{bootstrap_namespace, resolve_path, submit_commit};
use crate::common::namespace_engine;
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, AccessRights, CommitId,
    DestinationBehavior, ErrorCode, InodeId, NamespaceAccess, NamespaceId, PrincipalId,
    PrincipalScope, ROOT_INODE_ID,
};
use loonfs_core::publish::{CommitRequest, FilesystemOperation};
use loonfs_core::{BootstrapOptions, MutationContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

fn grants(principal: &str, rights: &[AccessRight]) -> AccessGrants {
    AccessGrants::new(std::collections::BTreeMap::from([(
        PrincipalId::parse(principal).expect("principal"),
        AccessRights::from_iter(rights.iter().copied()),
    )]))
    .expect("grants")
}

fn update_access(path: &str, boundary: bool, grants: AccessGrants) -> FilesystemOperation {
    FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse(path).expect("path"),
        boundary,
        grants,
        expected_inode_id: None,
        expected_access_revision_no: None,
    }
}

async fn setup() -> (
    tempfile::TempDir,
    LocalFsStore,
    NamespaceId,
    MutationContext,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = crate::common::mutation_context("writer", 1);
    namespace_engine(&store, &namespace_id, &context)
        .bootstrap_namespace(BootstrapOptions {
            access: NamespaceAccess::Acl {
                principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                root_grants: grants("prn_root", &[AccessRight::Admin]),
            },
            ..BootstrapOptions::new(loonfs_test_support::test_actor())
        })
        .await
        .expect("bootstrap ACL namespace");
    (temp_dir, store, namespace_id, context)
}

fn event(
    inode_id: InodeId,
    revision: u64,
    boundary: bool,
    grants: AccessGrants,
) -> FilesystemChange {
    FilesystemChange::AccessChanged {
        inode_id,
        access_revision_no: AccessRevisionNo(revision),
        boundary,
        grants,
    }
}

#[tokio::test]
async fn an_update_replaces_the_row_and_advances_the_revision() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let root_grants = access_grants(&[
        ("prn_root", &[AccessRight::Admin]),
        ("prn_team", &[AccessRight::Read, AccessRight::Write]),
    ]);
    let root = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("root-access").expect("commit id"),
        update_access("/", false, root_grants.clone()),
        &context,
    )
    .await
    .expect("root access");
    assert_eq!(
        root.events,
        Some(vec![event(ROOT_INODE_ID, 1, false, root_grants)])
    );

    commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        create_directory("/docs"),
    )
    .await
    .expect("directory");
    let directory_inode = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve directory")
        .inode_id;
    let directory_grants = grants("prn_finance", &[AccessRight::Read]);
    let directory = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("directory-access").expect("commit id"),
        update_access("/docs", true, directory_grants.clone()),
        &context,
    )
    .await
    .expect("directory access");
    assert_eq!(
        directory.events,
        Some(vec![event(
            directory_inode,
            1,
            true,
            directory_grants.clone()
        )])
    );

    let stale = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("stale-root-access").expect("commit id"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::default(),
            expected_inode_id: Some(ROOT_INODE_ID),
            expected_access_revision_no: Some(AccessRevisionNo(0)),
        },
        &context,
    )
    .await
    .expect_err("stale root revision");
    assert_eq!(stale.code(), ErrorCode::StaleAccess);
    let details = stale.details().expect("stale details");
    assert_eq!(
        details.expected_access_revision_no,
        Some(AccessRevisionNo(0))
    );
    assert_eq!(details.actual_access_revision_no, Some(AccessRevisionNo(1)));

    commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        create_directory("/batch"),
    )
    .await
    .expect("batch directory");
    let batch_inode = resolve_path(&store, &namespace_id, "/batch")
        .await
        .expect("resolve batch directory")
        .inode_id;
    let batch = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: CommitId::parse("batch-access").expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: Some(subject("root", &["prn_root"])),
            message: None,
            preconditions: Vec::new(),
            operations: vec![
                update_access("/batch", true, directory_grants.clone()),
                update_access("/batch", false, AccessGrants::default()),
            ],
        },
        &context,
    )
    .await
    .expect("sequential access updates");
    assert_eq!(
        batch.events,
        Some(vec![
            event(batch_inode, 1, true, directory_grants),
            event(batch_inode, 2, false, AccessGrants::default()),
        ])
    );
}

#[tokio::test]
async fn structural_rules_reject_admin_off_the_root_and_a_boundary_on_a_file() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let content = loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"body")
        .await
        .expect("content")
        .into_content_ref();
    seed(
        &store,
        &namespace_id,
        &context,
        vec![put("/docs/file", &content)],
    )
    .await;
    for (id, operation) in [
        (
            "admin-off-root",
            update_access("/docs", false, grants("prn_root", &[AccessRight::Admin])),
        ),
        (
            "file-boundary",
            update_access("/docs/file", true, AccessGrants::default()),
        ),
    ] {
        let error = submit_operation(
            &store,
            &namespace_id,
            CommitId::parse(id).expect("commit id"),
            operation,
            &context,
        )
        .await
        .expect_err("invalid access update");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
}

#[tokio::test]
async fn an_unrestricted_namespace_refuses_access_updates() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = crate::common::mutation_context("writer", 1);
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("unrestricted-access").expect("commit id"),
        update_access("/", false, AccessGrants::default()),
        &context,
    )
    .await
    .expect_err("unrestricted namespace");
    assert_eq!(error.code(), ErrorCode::NamespaceUnrestricted);
}

#[tokio::test]
async fn access_rows_survive_a_flush_and_the_counter_keeps_going() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("before-flush").expect("commit id"),
        update_access("/", false, grants("prn_root", &[AccessRight::Admin])),
        &context,
    )
    .await
    .expect("root access");
    namespace_engine(&store, &namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush");
    let after = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("after-flush").expect("commit id"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::default(),
            expected_inode_id: Some(ROOT_INODE_ID),
            expected_access_revision_no: Some(AccessRevisionNo(1)),
        },
        &context,
    )
    .await
    .expect("update after flush");
    assert_eq!(
        after.events,
        Some(vec![event(
            ROOT_INODE_ID,
            2,
            false,
            AccessGrants::default()
        )])
    );
}

fn subject(id: &str, principals: &[&str]) -> loonfs_api::Subject {
    loonfs_api::Subject {
        subject_id: loonfs_api::SubjectId::parse(id).expect("subject id"),
        principals: loonfs_api::PrincipalSet::new(
            principals
                .iter()
                .map(|id| PrincipalId::parse(id).expect("principal id"))
                .collect(),
        )
        .expect("principals"),
    }
}

fn access_grants(entries: &[(&str, &[AccessRight])]) -> AccessGrants {
    AccessGrants::new(
        entries
            .iter()
            .map(|(id, rights)| {
                (
                    PrincipalId::parse(id).expect("principal"),
                    AccessRights::from_iter(rights.iter().copied()),
                )
            })
            .collect(),
    )
    .expect("grants")
}

fn create_directory(path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(path).expect("path"),
        parents: false,
    }
}

fn put(path: &str, content_ref: &loonfs_api::ContentRef) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content_ref: content_ref.clone(),
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

fn delete(path: &str) -> FilesystemOperation {
    FilesystemOperation::DeletePath {
        path: AbsolutePath::parse(path).expect("path"),
        behavior: loonfs_api::DeleteDirectoryBehavior::NonRecursive,
        expected_inode_id: None,
    }
}

fn move_path(from: &str, to: &str) -> FilesystemOperation {
    FilesystemOperation::MovePath {
        source_path: AbsolutePath::parse(from).expect("source"),
        destination_path: AbsolutePath::parse(to).expect("destination"),
        precondition: loonfs_api::DestinationPrecondition {
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    }
}

async fn commit_as(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    subject: loonfs_api::Subject,
    operation: FilesystemOperation,
) -> Result<loonfs_api::Commit, loonfs_core::Error> {
    submit_commit(
        store,
        namespace_id,
        CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            operation,
        )
        .with_subject(subject),
        context,
    )
    .await
}

async fn seed(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    operations: Vec<FilesystemOperation>,
) {
    for operation in operations {
        commit_as(
            store,
            namespace_id,
            context,
            subject("root", &["prn_root"]),
            operation,
        )
        .await
        .expect("seed operation");
    }
}

#[tokio::test]
async fn an_acl_namespace_requires_a_subject_and_an_unrestricted_one_ignores_it() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let request = CommitRequest::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        create_directory("/docs"),
    );
    let error = submit_commit(&store, &namespace_id, request.clone(), &context)
        .await
        .expect_err("subject required");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    submit_commit(
        &store,
        &namespace_id,
        request.with_subject(subject("root", &["prn_root"])),
        &context,
    )
    .await
    .expect("administrator commits");
    let unrestricted = NamespaceId::parse("unrestricted").expect("namespace");
    bootstrap_namespace(&store, &unrestricted, &context)
        .await
        .expect("bootstrap");
    commit_as(
        &store,
        &unrestricted,
        &context,
        subject("any", &["any"]),
        create_directory("/docs"),
    )
    .await
    .expect("subject ignored");
}

#[tokio::test]
async fn rights_gate_operations_and_absence_hides_the_inode() {
    use AccessRight::{Create, Read, Remove, Write};
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let content = loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"body")
        .await
        .expect("content")
        .into_content_ref();
    seed(
        &store,
        &namespace_id,
        &context,
        vec![
            create_directory("/team"),
            create_directory("/inbox"),
            update_access(
                "/team",
                false,
                access_grants(&[
                    ("team", &[Read, Write, Create, Remove]),
                    ("viewer", &[Read]),
                ]),
            ),
            update_access("/inbox", false, grants("uploader", &[Create])),
            put("/team/delete", &content),
            put("/team/kept", &content),
        ],
    )
    .await;
    let kept = resolve_path(&store, &namespace_id, "/team/kept")
        .await
        .expect("kept");
    let deleted = resolve_path(&store, &namespace_id, "/team/delete")
        .await
        .expect("delete");
    let wrong_inode = InodeId(kept.inode_id.0 + 1);
    for (principal, operation, expected) in [
        (
            "viewer",
            put("/team/new", &content),
            Some(ErrorCode::Forbidden),
        ),
        (
            "stranger",
            put("/team/new", &content),
            Some(ErrorCode::PathNotFound),
        ),
        ("uploader", put("/inbox/new", &content), None),
        (
            "uploader",
            put("/inbox/new", &content),
            Some(ErrorCode::PathConflict),
        ),
        ("team", delete("/team/delete"), None),
        ("viewer", delete("/team/kept"), Some(ErrorCode::Forbidden)),
        (
            "stranger",
            FilesystemOperation::DeleteByInode {
                inode_id: kept.inode_id,
                expected_binding_generation: kept.binding_generation.expect("binding"),
                behavior: loonfs_api::DeleteDirectoryBehavior::NonRecursive,
            },
            Some(ErrorCode::InodeNotFound),
        ),
        (
            "stranger",
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/team/kept").expect("path"),
                behavior: loonfs_api::DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: Some(wrong_inode),
            },
            Some(ErrorCode::PathNotFound),
        ),
        (
            "stranger",
            FilesystemOperation::UpdateAttributes {
                path: AbsolutePath::parse("/team/kept").expect("path"),
                set: std::collections::BTreeMap::from([(
                    loonfs_api::AttributeKey::parse("owner").expect("key"),
                    loonfs_api::AttributeValue::parse("ada").expect("value"),
                )]),
                remove: Vec::new(),
                expected_inode_id: Some(wrong_inode),
                expected_attributes_revision_no: Some(loonfs_api::AttributeRevisionNo(0)),
            },
            Some(ErrorCode::PathNotFound),
        ),
        (
            "stranger",
            FilesystemOperation::UpdateAccess {
                path: AbsolutePath::parse("/team/kept").expect("path"),
                boundary: false,
                grants: AccessGrants::default(),
                expected_inode_id: Some(wrong_inode),
                expected_access_revision_no: Some(AccessRevisionNo(0)),
            },
            Some(ErrorCode::PathNotFound),
        ),
        (
            "stranger",
            FilesystemOperation::DeleteByInode {
                inode_id: kept.inode_id,
                expected_binding_generation: deleted.binding_generation.expect("binding"),
                behavior: loonfs_api::DeleteDirectoryBehavior::NonRecursive,
            },
            Some(ErrorCode::InodeNotFound),
        ),
    ] {
        let result = commit_as(
            &store,
            &namespace_id,
            &context,
            subject(principal, &[principal]),
            operation,
        )
        .await;
        assert_eq!(
            result.map(|_| ()).map_err(|error| error.code()),
            expected.map_or(Ok(()), Err),
            "{principal}"
        );
    }
}

#[tokio::test]
async fn moves_are_authorized_as_the_equivalent_grant() {
    use AccessRight::{Admin, Create, Manage, Read, Remove, Share, Write};
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let content = loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"body")
        .await
        .expect("content")
        .into_content_ref();
    let editors = [Read, Write, Create, Remove];
    let secret = access_grants(&[
        ("editor", &editors),
        ("manager", &editors),
        ("entry", &[Create, Share, Remove]),
    ]);
    let team = access_grants(&[
        ("editor", &editors),
        ("manager", &editors),
        ("entry", &[Create, Share, Remove]),
        ("bob", &[Read]),
    ]);
    seed(
        &store,
        &namespace_id,
        &context,
        vec![
            update_access(
                "/",
                false,
                access_grants(&[("prn_root", &[Admin]), ("ops", &[Admin])]),
            ),
            create_directory("/team"),
            create_directory("/secret"),
            create_directory("/admin-source"),
            create_directory("/admin-target"),
            update_access("/team", false, team),
            update_access("/secret", true, secret),
            update_access("/admin-source", true, grants("editor", &editors)),
            update_access(
                "/admin-target",
                true,
                access_grants(&[("editor", &editors), ("ops", &[Read])]),
            ),
            put("/team/rename", &content),
            put("/secret/shared", &content),
            put("/secret/managed", &content),
            put("/secret/entry", &content),
            create_directory("/secret/folder"),
            update_access("/secret/folder", true, AccessGrants::default()),
            put("/admin-source/file", &content),
            put("/secret/recover", &content),
            put("/team/loss", &content),
            update_access("/secret/managed", false, grants("manager", &[Manage])),
        ],
    )
    .await;
    let recover = resolve_path(&store, &namespace_id, "/secret/recover")
        .await
        .expect("recover inode");
    let deletion = commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        delete("/secret/recover"),
    )
    .await
    .expect("delete");
    let undelete = FilesystemOperation::Undelete {
        inode_id: recover.inode_id,
        deletion_seq: deletion.committed_seq,
        destination_path: Some(AbsolutePath::parse("/team/recovered").expect("path")),
    };
    for (principal, operation, expected) in [
        ("editor", move_path("/team/rename", "/team/renamed"), None),
        (
            "editor",
            move_path("/secret/shared", "/team/shared"),
            Some(ErrorCode::Forbidden),
        ),
        (
            "prn_root",
            update_access("/secret/shared", false, grants("editor", &[Share])),
            None,
        ),
        ("editor", move_path("/secret/shared", "/team/shared"), None),
        (
            "manager",
            move_path("/secret/managed", "/team/managed"),
            None,
        ),
        (
            "entry",
            move_path("/secret/entry", "/team/entry"),
            Some(ErrorCode::Forbidden),
        ),
        ("editor", move_path("/secret/folder", "/team/folder"), None),
        (
            "editor",
            move_path("/admin-source/file", "/admin-target/file"),
            None,
        ),
        ("editor", undelete.clone(), Some(ErrorCode::Forbidden)),
        ("prn_root", undelete, None),
        ("editor", move_path("/team/loss", "/secret/loss"), None),
    ] {
        let result = commit_as(
            &store,
            &namespace_id,
            &context,
            subject(principal, &[principal]),
            operation,
        )
        .await;
        assert_eq!(
            result.map(|_| ()).map_err(|error| error.code()),
            expected.map_or(Ok(()), Err),
            "{principal}"
        );
    }
}

#[tokio::test]
async fn access_updates_are_authorized_by_what_they_change() {
    use AccessRight::{Admin, Manage, Read, Share};
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let root = access_grants(&[("prn_root", &[Admin]), ("manager", &[Manage])]);
    let shared = access_grants(&[("sharer", &[Read, Share]), ("reader", &[Read])]);
    seed(
        &store,
        &namespace_id,
        &context,
        vec![
            update_access("/", false, root.clone()),
            create_directory("/docs"),
            update_access("/docs", false, grants("sharer", &[Read, Share])),
        ],
    )
    .await;
    for (principal, operation, expected) in [
        (
            "manager",
            update_access("/", false, grants("manager", &[Manage])),
            Some(ErrorCode::Forbidden),
        ),
        ("manager", update_access("/", false, root), None),
        (
            "prn_root",
            update_access(
                "/",
                false,
                access_grants(&[
                    ("prn_root", &[Admin]),
                    ("manager", &[Manage]),
                    ("new-admin", &[Admin]),
                ]),
            ),
            None,
        ),
        (
            "sharer",
            update_access("/docs", false, shared.clone()),
            None,
        ),
        (
            "sharer",
            update_access(
                "/docs",
                false,
                access_grants(&[("sharer", &[Read, Share]), ("reader", &[Manage])]),
            ),
            Some(ErrorCode::Forbidden),
        ),
        (
            "sharer",
            update_access("/docs", true, shared.clone()),
            Some(ErrorCode::Forbidden),
        ),
        ("manager", update_access("/docs", true, shared), None),
    ] {
        let result = commit_as(
            &store,
            &namespace_id,
            &context,
            subject(principal, &[principal]),
            operation,
        )
        .await;
        assert_eq!(
            result.map(|_| ()).map_err(|error| error.code()),
            expected.map_or(Ok(()), Err),
            "{principal}"
        );
    }
}

#[tokio::test]
async fn a_retry_by_another_subject_is_a_reuse_conflict() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    seed(
        &store,
        &namespace_id,
        &context,
        vec![
            create_directory("/docs"),
            update_access("/docs", false, grants("creator", &[AccessRight::Create])),
        ],
    )
    .await;
    let request = CommitRequest::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        create_directory("/docs/child"),
    )
    .with_subject(subject("ada", &["creator"]));
    let receipt = submit_commit(&store, &namespace_id, request.clone(), &context)
        .await
        .expect("commit");
    let error = submit_commit(
        &store,
        &namespace_id,
        request.clone().with_subject(subject("bob", &["creator"])),
        &context,
    )
    .await
    .expect_err("reuse conflict");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    seed(
        &store,
        &namespace_id,
        &context,
        vec![update_access("/docs", false, AccessGrants::default())],
    )
    .await;
    assert_eq!(
        submit_commit(&store, &namespace_id, request, &context)
            .await
            .expect("original receipt"),
        receipt
    );
}

#[tokio::test]
async fn upload_sessions_belong_to_their_subject() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let engine = namespace_engine(&store, &namespace_id, &context);
    assert_eq!(
        engine
            .begin_upload(None)
            .await
            .expect_err("subject required")
            .code(),
        ErrorCode::InvalidRequest
    );
    let ada = loonfs_api::SubjectId::parse("usr_ada").expect("subject");
    let bob = loonfs_api::SubjectId::parse("usr_bob").expect("subject");
    let session = engine.begin_upload(Some(&ada)).await.expect("begin");
    assert_eq!(
        engine
            .get_upload_status(&session.upload_id, Some(&bob))
            .await
            .expect_err("hidden session")
            .code(),
        ErrorCode::UploadNotFound
    );
    engine
        .get_upload_status(&session.upload_id, Some(&ada))
        .await
        .expect("owner session");
    let unrestricted = NamespaceId::parse("unrestricted").expect("namespace");
    bootstrap_namespace(&store, &unrestricted, &context)
        .await
        .expect("bootstrap");
    let engine = namespace_engine(&store, &unrestricted, &context);
    let session = engine.begin_upload(Some(&ada)).await.expect("begin");
    engine
        .get_upload_status(&session.upload_id, None)
        .await
        .expect("subject ignored");
}

async fn submit_operation(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    commit_id: CommitId,
    operation: FilesystemOperation,
    context: &MutationContext,
) -> Result<loonfs_api::Commit, loonfs_core::Error> {
    submit_commit(
        store,
        namespace_id,
        CommitRequest::single(
            commit_id,
            loonfs_test_support::test_actor(),
            None,
            operation,
        )
        .with_subject(subject("root", &["prn_root"])),
        context,
    )
    .await
}
