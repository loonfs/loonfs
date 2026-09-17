//! Access updates, structural rules, and revision continuity after a flush.

use crate::common::commit_split_support::{
    bootstrap_namespace, create_directory_path, put_file_bytes, resolve_path, submit_commit,
    submit_operation,
};
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
    let root_grants = grants("prn_team", &[AccessRight::Read, AccessRight::Write]);
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

    create_directory_path(&store, &namespace_id, "/docs", &context, Some("docs"))
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

    create_directory_path(
        &store,
        &namespace_id,
        "/batch",
        &context,
        Some("batch-directory"),
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
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/file",
        b"body",
        DestinationBehavior::NoReplace,
        &context,
        Some("file"),
    )
    .await
    .expect("file");
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
