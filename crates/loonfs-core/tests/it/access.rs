//! Access updates, structural rules, and revision continuity after a flush.

use crate::common::commit_split_support::{bootstrap_namespace, submit_commit};
use crate::common::{namespace_engine, read_context};
use loonfs_api::options::{ListPathEntriesOptions, StatPathOptions};
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, AccessRights, CommitId,
    CommitPrecondition, DestinationBehavior, DisplayName, ErrorCode, InodeId, NamespaceAccess,
    NamespaceId, PrincipalId, PrincipalScope, ROOT_INODE_ID,
};
use loonfs_api::{
    AttributeInclusion, ChangeSeq, ContentRef, Page, PageRequest, PaginationPolicy, RevisionNo,
    TrashEntry, TrashPageCursor,
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
        vec![event(ROOT_INODE_ID, 1, false, root_grants)]
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
        vec![event(directory_inode, 1, true, directory_grants.clone())]
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
        vec![
            event(batch_inode, 1, true, directory_grants),
            event(batch_inode, 2, false, AccessGrants::default()),
        ]
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
        vec![event(ROOT_INODE_ID, 2, false, AccessGrants::default())]
    );
}

fn subject(id: &str, principals: &[&str]) -> loonfs_api::Subject {
    loonfs_api::Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
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
        content_ref: Some(content_ref.clone()),
        inline_content: None,
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
                expected_binding_version: kept.binding_version.expect("binding"),
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
                expected_attributes_revision_no: Some(loonfs_api::AttributesRevisionNo(0)),
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
                expected_binding_version: deleted.binding_version.expect("binding"),
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
async fn a_replay_from_another_principal_scope_is_refused() {
    let (_temp_dir, store, namespace_id, context) = setup().await;
    let request = CommitRequest::single(
        CommitId::parse("scope-replay").expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        create_directory("/docs"),
    )
    .with_subject(subject("root", &["prn_root"]));
    submit_commit(&store, &namespace_id, request.clone(), &context)
        .await
        .expect("commit");

    let mut wrong_scope = subject("root", &["prn_root"]);
    wrong_scope.principal_scope = PrincipalScope::parse("org_other").expect("scope");
    let error = submit_commit(
        &store,
        &namespace_id,
        request.with_subject(wrong_scope),
        &context,
    )
    .await
    .expect_err("wrong-scope replay");
    assert!(matches!(
        error,
        loonfs_core::Error::PrincipalScopeMismatch {
            expected_principal_scope,
            actual_principal_scope,
        } if expected_principal_scope.as_str() == "org_demo"
            && actual_principal_scope.as_str() == "org_other"
    ));
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
    let ada = subject("usr_ada", &[]);
    let bob = subject("usr_bob", &[]);
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

fn read_engine<'a>(
    store: &'a LocalFsStore,
    namespace_id: &NamespaceId,
    principal: &str,
) -> loonfs_core::NamespaceReaderEngine<&'a LocalFsStore> {
    loonfs_core::NamespaceReaderEngine::reader(store, namespace_id.clone())
        .with_subject(subject(principal, &[principal]))
}

async fn resolve_path(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    path: &str,
) -> Result<loonfs_api::PathEntry, loonfs_core::Error> {
    read_engine(store, namespace_id, "prn_root")
        .resolve_path(
            path,
            StatPathOptions::default(),
            &read_context(store, namespace_id).await,
        )
        .await
}

fn read_page<C>(limit: u32) -> PageRequest<C> {
    PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(Some(limit))
            .expect("limit"),
        cursor: None,
    }
}

async fn read_fixture() -> (
    tempfile::TempDir,
    LocalFsStore,
    NamespaceId,
    MutationContext,
    ContentRef,
) {
    use AccessRight::{Create, History, Read, Remove, Write};
    let (temp_dir, store, namespace_id, context) = setup().await;
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
            create_directory("/team/secret"),
            create_directory("/inbox"),
            update_access(
                "/team",
                false,
                access_grants(&[
                    ("team", &[Read, Write, Create, Remove]),
                    ("viewer", &[Read]),
                    ("writer", &[Write]),
                    ("historian", &[Read, History]),
                ]),
            ),
            update_access("/team/secret", true, grants("finance", &[Read, History])),
            update_access("/inbox", false, grants("uploader", &[Create])),
            put("/team/file", &content),
            put("/team/kept", &content),
            put("/team/secret/file", &content),
            put("/inbox/file", &content),
        ],
    )
    .await;
    (temp_dir, store, namespace_id, context, content)
}

#[tokio::test]
async fn reads_require_read_and_absence_hides_the_inode() {
    let (_temp_dir, store, namespace_id, _, _) = read_fixture().await;
    let context = read_context(&store, &namespace_id).await;
    let viewer = read_engine(&store, &namespace_id, "viewer");
    let entry = viewer
        .resolve_path("/team/file", StatPathOptions::default(), &context)
        .await
        .expect("stat");
    assert_eq!(
        viewer
            .get_file("/team/file", &context, None)
            .await
            .expect("content")
            .bytes,
        b"body"
    );
    let listing = viewer
        .list_path_page(
            "/team",
            read_page(10),
            ListPathEntriesOptions {
                include_attributes: AttributeInclusion::Include,
                ..Default::default()
            },
            &context,
        )
        .await
        .expect("list");
    assert_eq!(listing.items.len(), 3);
    for entry in listing.items {
        assert_eq!(
            entry.attributes.is_some(),
            entry.path.as_str() != "/team/secret"
        );
    }
    for (principal, path, inode_id, path_error, inode_error) in [
        (
            "stranger",
            "/team/file",
            entry.inode_id,
            Some(ErrorCode::PathNotFound),
            Some(ErrorCode::InodeNotFound),
        ),
        (
            "writer",
            "/team/file",
            entry.inode_id,
            Some(ErrorCode::Forbidden),
            Some(ErrorCode::Forbidden),
        ),
        ("viewer", "/team/file", entry.inode_id, None, None),
        (
            "viewer",
            "/team/absent",
            InodeId(999),
            Some(ErrorCode::PathNotFound),
            Some(ErrorCode::InodeNotFound),
        ),
    ] {
        let reader = read_engine(&store, &namespace_id, principal);
        let path_result = reader
            .resolve_path(path, StatPathOptions::default(), &context)
            .await
            .map(|entry| entry.inode_id)
            .map_err(|error| error.code());
        let inode_result = reader
            .stat_inode(inode_id, StatPathOptions::default(), &context)
            .await
            .map(|entry| entry.inode_id)
            .map_err(|error| error.code());
        assert_eq!(
            path_result,
            path_error.map_or(Ok(inode_id), Err),
            "{principal}: {path}"
        );
        assert_eq!(
            inode_result,
            inode_error.map_or(Ok(inode_id), Err),
            "{principal}: {inode_id}"
        );
    }
    let stranger = read_engine(&store, &namespace_id, "stranger");
    assert_eq!(
        stranger
            .list_path_page(
                "/team",
                read_page(10),
                ListPathEntriesOptions::default(),
                &context
            )
            .await
            .expect_err("hidden directory")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        stranger
            .get_file("/team/file", &context, None)
            .await
            .expect_err("hidden content")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        read_engine(&store, &namespace_id, "uploader")
            .resolve_path("/inbox", StatPathOptions::default(), &context)
            .await
            .expect_err("create is not read")
            .code(),
        ErrorCode::Forbidden
    );
}

#[tokio::test]
async fn history_needs_the_history_right() {
    let (_temp_dir, store, namespace_id, mutation, content) = read_fixture().await;
    seed(
        &store,
        &namespace_id,
        &mutation,
        vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/team/file").expect("path"),
            content_ref: Some(content),
            inline_content: None,
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        }],
    )
    .await;
    let context = read_context(&store, &namespace_id).await;
    let inode_id = resolve_path(&store, &namespace_id, "/team/file")
        .await
        .expect("file")
        .inode_id;
    for (principal, history) in [("viewer", false), ("historian", true)] {
        let engine = read_engine(&store, &namespace_id, principal);
        for revision in [RevisionNo(1), RevisionNo(2)] {
            let expected = if history || revision == RevisionNo(2) {
                Ok(())
            } else {
                Err(ErrorCode::Forbidden)
            };
            assert_eq!(
                engine
                    .get_file_revision("/team/file", revision, &context, None)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.code()),
                expected
            );
            assert_eq!(
                engine
                    .get_file_revision_for_inode(inode_id, revision, &context, None)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.code()),
                expected
            );
            assert_eq!(
                engine
                    .direct_download_target("/team/file", Some(revision), &context)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.code()),
                expected
            );
            assert_eq!(
                engine
                    .direct_download_target_by_inode(inode_id, revision, &context)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.code()),
                expected
            );
        }
        let expected = if history {
            Ok(2)
        } else {
            Err(ErrorCode::Forbidden)
        };
        assert_eq!(
            engine
                .list_file_revisions_page("/team/file", read_page(10), &context)
                .await
                .map(|(_, page)| page.items.len())
                .map_err(|error| error.code()),
            expected
        );
        assert_eq!(
            engine
                .list_file_revisions_for_inode_page(inode_id, read_page(10), &context)
                .await
                .map(|page| page.items.len())
                .map_err(|error| error.code()),
            expected
        );
    }
}

#[tokio::test]
async fn snapshot_reads_authorize_the_historical_inode_at_head() {
    let (_temp_dir, store, namespace_id, mutation, _) = read_fixture().await;
    let snapshot = read_context(&store, &namespace_id).await;
    seed(
        &store,
        &namespace_id,
        &mutation,
        vec![move_path("/team/file", "/team/secret/moved")],
    )
    .await;
    let head = read_context(&store, &namespace_id).await;
    let viewer = read_engine(&store, &namespace_id, "viewer").with_authorization_head(head.clone());
    assert_eq!(
        viewer
            .get_file("/team/file", &snapshot, None)
            .await
            .expect_err("moved historical inode is hidden")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        viewer
            .get_file("/team/kept", &snapshot, None)
            .await
            .expect_err("snapshot needs history")
            .code(),
        ErrorCode::Forbidden
    );
    let finance =
        read_engine(&store, &namespace_id, "finance").with_authorization_head(head.clone());
    assert_eq!(
        finance
            .get_file("/team/file", &snapshot, None)
            .await
            .expect("historical inode")
            .bytes,
        b"body"
    );
    let historian = read_engine(&store, &namespace_id, "historian").with_authorization_head(head);
    let listing = historian
        .list_path_page(
            "/team",
            read_page(10),
            ListPathEntriesOptions::default(),
            &snapshot,
        )
        .await
        .expect("historical names");
    assert_eq!(listing.items.len(), 3);
}

async fn trash_page(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    principal: &str,
    limit: u32,
    cursor: Option<TrashPageCursor>,
) -> Page<TrashEntry, TrashPageCursor> {
    let mut request = read_page(limit);
    request.cursor = cursor;
    read_engine(store, namespace_id, principal)
        .list_trash_page(request, &read_context(store, namespace_id).await)
        .await
        .expect("trash page")
}

fn deleted_names(page: &Page<TrashEntry, TrashPageCursor>) -> Vec<String> {
    page.items
        .iter()
        .map(|entry| entry.deleted_binding.display_name.to_string())
        .collect()
}

#[tokio::test]
async fn trash_lists_entries_whose_original_parent_is_readable() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    seed(
        &store,
        &namespace_id,
        &context,
        vec![delete("/team/kept"), delete("/team/secret/file")],
    )
    .await;
    assert_eq!(
        deleted_names(&trash_page(&store, &namespace_id, "viewer", 10, None).await),
        ["kept"]
    );
    assert_eq!(
        deleted_names(&trash_page(&store, &namespace_id, "finance", 10, None).await),
        ["file"]
    );
    assert_eq!(
        trash_page(&store, &namespace_id, "prn_root", 10, None)
            .await
            .items
            .len(),
        2
    );
    let first = trash_page(&store, &namespace_id, "viewer", 1, None).await;
    assert_eq!(deleted_names(&first), ["kept"]);
    let cursor = first.next_cursor.expect("a filtered page keeps its cursor");
    let last = trash_page(&store, &namespace_id, "viewer", 1, Some(cursor)).await;
    assert!(last.items.is_empty());
    assert!(last.next_cursor.is_none());
}

#[tokio::test]
async fn the_feed_and_content_refs_need_an_administrator_or_no_subject() {
    let (_temp_dir, store, namespace_id, _, content) = read_fixture().await;
    let context = read_context(&store, &namespace_id).await;
    let viewer = read_engine(&store, &namespace_id, "viewer");
    assert_eq!(
        viewer
            .require_administrator(&context)
            .await
            .expect_err("feed requires administrator")
            .code(),
        ErrorCode::Forbidden
    );
    assert_eq!(
        viewer
            .read_content_ref(&content, 100, &context)
            .await
            .expect_err("bare content requires administrator")
            .code(),
        ErrorCode::Forbidden
    );
    for engine in [
        read_engine(&store, &namespace_id, "prn_root"),
        loonfs_core::NamespaceReaderEngine::reader(&store, namespace_id.clone()),
    ] {
        engine
            .require_administrator(&context)
            .await
            .expect("feed authorized");
        assert!(!engine
            .list_changes_after(ChangeSeq(0), read_page::<()>(100).limit, &context)
            .await
            .expect("feed")
            .changes
            .is_empty());
        assert_eq!(
            engine
                .read_content_ref(&content, 100, &context)
                .await
                .expect("content"),
            b"body"
        );
    }
    let engine = loonfs_core::NamespaceReaderEngine::reader(&store, namespace_id);
    let error = engine
        .resolve_path("/team/file", StatPathOptions::default(), &context)
        .await
        .expect_err("subject required");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert!(matches!(error, loonfs_core::Error::SubjectRequired { .. }));
}

#[tokio::test]
async fn a_revocation_is_visible_to_the_next_read() {
    let (_temp_dir, store, namespace_id, mutation, _) = read_fixture().await;
    let viewer = read_engine(&store, &namespace_id, "viewer");
    viewer
        .resolve_path(
            "/team/file",
            StatPathOptions::default(),
            &read_context(&store, &namespace_id).await,
        )
        .await
        .expect("before revocation");
    seed(
        &store,
        &namespace_id,
        &mutation,
        vec![update_access(
            "/team",
            false,
            grants("team", &[AccessRight::Read]),
        )],
    )
    .await;
    for flush in [false, true] {
        if flush {
            namespace_engine(&store, &namespace_id, &mutation)
                .flush_wal()
                .await
                .expect("flush");
        }
        assert_eq!(
            viewer
                .resolve_path(
                    "/team/file",
                    StatPathOptions::default(),
                    &read_context(&store, &namespace_id).await
                )
                .await
                .expect_err("revoked")
                .code(),
            ErrorCode::PathNotFound
        );
    }
}

#[tokio::test]
async fn unreadable_access_targets_are_hidden_before_structural_errors() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for path in ["/team/secret/file", "/team/secret"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("viewer", &["viewer"]),
            update_access(path, true, AccessGrants::default()),
        )
        .await
        .expect_err("unreadable access target");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }
    for path in ["/team/secret/file/child", "/team/secret/child"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("viewer", &["viewer"]),
            update_access(path, true, AccessGrants::default()),
        )
        .await
        .expect_err("unreadable access parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }

    let error = commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        update_access("/team/secret/file", true, AccessGrants::default()),
    )
    .await
    .expect_err("administrator receives the structural error");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        error.to_string(),
        "invalid commit request: a boundary applies only to a directory"
    );
}

#[tokio::test]
async fn directory_creation_hides_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for parents in [false, true] {
        for path in ["/team/secret/file/child", "/team/secret/child"] {
            let error = commit_as(
                &store,
                &namespace_id,
                &context,
                subject("team", &["team"]),
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(path).expect("path"),
                    parents,
                },
            )
            .await
            .expect_err("unreadable parent");
            assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
        }
    }
}

#[tokio::test]
async fn path_put_hides_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, content) = read_fixture().await;
    for path in ["/team/secret/file/put", "/team/secret/put"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            put(path, &content),
        )
        .await
        .expect_err("unreadable parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }
}

#[tokio::test]
async fn inode_creation_hides_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    let hidden_directory = resolve_path(&store, &namespace_id, "/team/secret")
        .await
        .expect("hidden directory");
    let hidden_file = resolve_path(&store, &namespace_id, "/team/secret/file")
        .await
        .expect("hidden file");
    for parent_inode_id in [hidden_file.inode_id, hidden_directory.inode_id] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            FilesystemOperation::CreateDirectoryByInode {
                parent_inode_id,
                display_name: DisplayName::parse("child").expect("display name"),
            },
        )
        .await
        .expect_err("unreadable parent");
        assert_eq!(error.code(), ErrorCode::InodeNotFound, "{parent_inode_id}");
    }
}

#[tokio::test]
async fn path_move_hides_an_unreadable_destination_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for destination in ["/team/secret/file/moved", "/team/secret/moved"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            move_path("/team/file", destination),
        )
        .await
        .expect_err("unreadable destination parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{destination}");
    }
    for (source, destination) in [
        ("/team/secret/file/child", "/team/moved-from-file"),
        ("/team/secret/child", "/team/moved-from-directory"),
    ] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            move_path(source, destination),
        )
        .await
        .expect_err("unreadable source parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{source}");
    }
}

#[tokio::test]
async fn inode_move_authorizes_the_destination_before_state_errors() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    let source = resolve_path(&store, &namespace_id, "/team/file")
        .await
        .expect("source");
    let other = resolve_path(&store, &namespace_id, "/team/kept")
        .await
        .expect("other source");
    let hidden_directory = resolve_path(&store, &namespace_id, "/team/secret")
        .await
        .expect("hidden directory");
    let hidden_file = resolve_path(&store, &namespace_id, "/team/secret/file")
        .await
        .expect("hidden file");
    for (destination_parent_inode_id, expected_binding_version) in [
        (
            hidden_file.inode_id,
            source.binding_version.clone().expect("source binding"),
        ),
        (
            hidden_directory.inode_id,
            other.binding_version.expect("other binding"),
        ),
    ] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            FilesystemOperation::MoveByInode {
                inode_id: source.inode_id,
                expected_binding_version,
                destination_parent_inode_id,
                destination_display_name: DisplayName::parse("moved").expect("display name"),
                precondition: loonfs_api::DestinationPrecondition::default(),
            },
        )
        .await
        .expect_err("unreadable destination parent");
        assert_eq!(
            error.code(),
            ErrorCode::InodeNotFound,
            "{destination_parent_inode_id}"
        );
    }
}

#[tokio::test]
async fn path_copy_hides_an_unreadable_destination_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for destination in ["/team/secret/file/copied", "/team/secret/copied"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("viewer", &["viewer"]),
            FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse("/team/file").expect("source path"),
                destination_path: AbsolutePath::parse(destination).expect("destination path"),
                precondition: loonfs_api::DestinationPrecondition::default(),
            },
        )
        .await
        .expect_err("unreadable destination parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{destination}");
    }
    for (source, destination) in [
        ("/team/secret/file/child", "/team/copied-from-file"),
        ("/team/secret/child", "/team/copied-from-directory"),
    ] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("viewer", &["viewer"]),
            FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse(source).expect("source path"),
                destination_path: AbsolutePath::parse(destination).expect("destination path"),
                precondition: loonfs_api::DestinationPrecondition::default(),
            },
        )
        .await
        .expect_err("unreadable source parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{source}");
    }
}

#[tokio::test]
async fn path_delete_hides_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for path in ["/team/secret/file/child", "/team/secret/child"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            delete(path),
        )
        .await
        .expect_err("unreadable delete parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }
}

#[tokio::test]
async fn attribute_updates_hide_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for path in ["/team/secret/file/child", "/team/secret/child"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            FilesystemOperation::UpdateAttributes {
                path: AbsolutePath::parse(path).expect("path"),
                set: std::collections::BTreeMap::from([(
                    loonfs_api::AttributeKey::parse("owner").expect("key"),
                    loonfs_api::AttributeValue::parse("ada").expect("value"),
                )]),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            },
        )
        .await
        .expect_err("unreadable attribute parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }
}

#[tokio::test]
async fn revision_restores_hide_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for path in ["/team/secret/file/child", "/team/secret/child"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            FilesystemOperation::RestoreRevision {
                path: AbsolutePath::parse(path).expect("path"),
                source_revision_no: RevisionNo(1),
            },
        )
        .await
        .expect_err("unreadable restore parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{path}");
    }
}

#[tokio::test]
async fn undelete_authorizes_state_and_destination_before_errors() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    let hidden_directory = resolve_path(&store, &namespace_id, "/team/secret")
        .await
        .expect("hidden directory");
    let hidden_file = resolve_path(&store, &namespace_id, "/team/secret/file")
        .await
        .expect("hidden file");
    let visible_file = resolve_path(&store, &namespace_id, "/team/kept")
        .await
        .expect("visible file");
    let hidden_deletion = commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        delete("/team/secret/file"),
    )
    .await
    .expect("delete hidden file");
    let visible_deletion = commit_as(
        &store,
        &namespace_id,
        &context,
        subject("root", &["prn_root"]),
        delete("/team/kept"),
    )
    .await
    .expect("delete visible file");

    for (inode_id, deletion_seq) in [
        (hidden_file.inode_id, hidden_deletion.committed_seq),
        (hidden_directory.inode_id, hidden_deletion.committed_seq),
    ] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("stranger", &["stranger"]),
            FilesystemOperation::Undelete {
                inode_id,
                deletion_seq,
                destination_path: None,
            },
        )
        .await
        .expect_err("unreadable undelete target");
        assert_eq!(error.code(), ErrorCode::InodeNotFound, "{inode_id}");
    }

    for destination_path in ["/team/secret/file/restored", "/team/secret/restored"] {
        let error = commit_as(
            &store,
            &namespace_id,
            &context,
            subject("team", &["team"]),
            FilesystemOperation::Undelete {
                inode_id: visible_file.inode_id,
                deletion_seq: visible_deletion.committed_seq,
                destination_path: Some(AbsolutePath::parse(destination_path).expect("path")),
            },
        )
        .await
        .expect_err("unreadable undelete destination");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{destination_path}");
    }
}

#[tokio::test]
async fn path_absence_preconditions_hide_an_unreadable_parent_kind() {
    let (_temp_dir, store, namespace_id, context, _) = read_fixture().await;
    for (precondition_path, output_path) in [
        ("/team/secret/file/child", "/team/precondition-file"),
        ("/team/secret/child", "/team/precondition-directory"),
    ] {
        let request = CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            create_directory(output_path),
        )
        .with_subject(subject("team", &["team"]))
        .preconditions(vec![CommitPrecondition::PathAbsence {
            path: AbsolutePath::parse(precondition_path).expect("precondition path"),
        }]);
        let error = submit_commit(&store, &namespace_id, request, &context)
            .await
            .expect_err("unreadable precondition parent");
        assert_eq!(error.code(), ErrorCode::PathNotFound, "{precondition_path}");
    }
}
