//! Namespace administration and recovery under ACL access.

use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{
    CreateNamespaceOptions, CreateSnapshotOptions, DeleteNamespaceOptions, ForkNamespaceOptions,
    FsWriter, ListChangesOptions, StatPathOptions,
};
use loonfs_api::v0::FilesystemChange;
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, AccessRights, ChangeSeq, CommitId,
    ErrorCode, NamespaceAccess, NamespaceAccessMode, NamespaceId, PrincipalId, PrincipalScope,
    PrincipalSet, Subject, SubjectId, ROOT_INODE_ID,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn subject(name: &str, principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(name).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn root_grants(administrator: bool) -> AccessGrants {
    let mut grants = BTreeMap::from([(
        PrincipalId::parse("team").expect("principal"),
        AccessRights::from_iter([AccessRight::Read, AccessRight::Create]),
    )]);
    if administrator {
        grants.insert(
            PrincipalId::parse("prn_root").expect("principal"),
            AccessRights::from_iter([AccessRight::Admin]),
        );
    }
    AccessGrants::new(grants).expect("grants")
}

fn access_mode() -> NamespaceAccessMode {
    NamespaceAccessMode::Acl {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
    }
}

async fn create_namespace() -> (tempfile::TempDir, FsWriter, NamespaceId) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let writer =
        FsWriter::builder_with_store(Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")))
            .writer_id("access-namespace")
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("writer");
    let namespace_id = namespace_id("access-namespace");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: root_grants(true),
                },
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("namespace");
    (temp_dir, writer, namespace_id)
}

#[tokio::test]
async fn namespace_operations_need_an_administrator_or_no_subject() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let root = writer.as_subject(subject("root", "prn_root"));
    let member = writer.as_subject(subject("member", "team"));
    assert_eq!(
        writer
            .reader()
            .get_namespace(&namespace)
            .await
            .expect("namespace")
            .access,
        access_mode()
    );
    let fork = namespace_id("fork");
    let fork_options = ForkNamespaceOptions {
        actor_id: loonfs_test_support::test_actor(),
        snapshot_id: None,
    };
    assert_eq!(
        member
            .fork_namespace(&namespace, &fork, fork_options.clone())
            .await
            .expect_err("member fork")
            .code(),
        ErrorCode::Forbidden
    );
    root.fork_namespace(&namespace, &fork, fork_options)
        .await
        .expect("root fork");
    assert_eq!(
        writer
            .reader()
            .get_namespace(&fork)
            .await
            .expect("fork")
            .access,
        access_mode()
    );
    member
        .reader()
        .get_path_entry(&fork, "/", StatPathOptions::default())
        .await
        .expect("inherited member grant");
    let now_ms = loonfs_core::time::current_time_ms().expect("clock");
    let snapshot_options = CreateSnapshotOptions {
        name: "snapshot".to_owned(),
        expires_at_ms: now_ms + 60_000,
    };
    assert_eq!(
        member
            .create_snapshot_with_quota(&namespace, snapshot_options.clone(), now_ms, 10)
            .await
            .expect_err("member snapshot")
            .code(),
        ErrorCode::Forbidden
    );
    let snapshot = root
        .create_snapshot_with_quota(&namespace, snapshot_options, now_ms, 10)
        .await
        .expect("root snapshot");
    assert_eq!(
        member
            .delete_snapshot(&namespace, &snapshot.checkpoint_id.into())
            .await
            .expect_err("member snapshot delete")
            .code(),
        ErrorCode::Forbidden
    );
    let delete_options = DeleteNamespaceOptions::default();
    assert_eq!(
        member
            .delete_namespace(&namespace, delete_options)
            .await
            .expect_err("member namespace delete")
            .code(),
        ErrorCode::Forbidden
    );
    writer
        .delete_namespace(&namespace, delete_options)
        .await
        .expect("token holder delete");
}

async fn commit_as(
    writer: &FsWriter,
    namespace: &NamespaceId,
    subject: Subject,
    operation: FilesystemOperation,
) -> Result<loonfs_api::Commit, loonfs::RuntimeError> {
    writer
        .create_commit(
            namespace,
            CommitRequest::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                operation,
            )
            .with_subject(subject),
        )
        .await
}

#[tokio::test]
async fn subject_scope_is_enforced_only_for_acl_namespaces() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let mut wrong_scope = subject("root", "prn_root");
    wrong_scope.principal_scope = PrincipalScope::parse("org_other").expect("scope");
    let expected_message =
        "subject principal scope `org_other` does not match namespace principal scope `org_demo`";
    let error = writer
        .reader()
        .as_subject(wrong_scope.clone())
        .get_path_entry(&namespace, "/", StatPathOptions::default())
        .await
        .expect_err("wrong-scope read");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert_eq!(error.to_string(), expected_message);
    let error = commit_as(
        &writer,
        &namespace,
        wrong_scope.clone(),
        create_directory("/wrong"),
    )
    .await
    .expect_err("wrong-scope commit");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert_eq!(error.to_string(), expected_message);
    commit_as(
        &writer,
        &namespace,
        subject("root", "prn_root"),
        create_directory("/matching"),
    )
    .await
    .expect("matching-scope commit");

    let unrestricted = namespace_id("unrestricted-scope");
    writer
        .create_namespace(
            &unrestricted,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("unrestricted namespace");
    writer
        .reader()
        .as_subject(wrong_scope.clone())
        .get_path_entry(&unrestricted, "/", StatPathOptions::default())
        .await
        .expect("unrestricted read");
    commit_as(
        &writer,
        &unrestricted,
        wrong_scope,
        create_directory("/accepted"),
    )
    .await
    .expect("unrestricted commit");
}

fn create_directory(path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(path).expect("path"),
        parents: false,
    }
}

#[tokio::test]
async fn recovery_restores_an_administrator_and_keeps_the_other_root_grants() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    commit_as(
        &writer,
        &namespace,
        subject("root", "prn_root"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: root_grants(false),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
    )
    .await
    .expect("drop the administrator");
    assert_eq!(
        commit_as(
            &writer,
            &namespace,
            subject("root", "prn_root"),
            create_directory("/docs")
        )
        .await
        .expect_err("no rights left")
        .code(),
        ErrorCode::PathNotFound
    );
    let principal = PrincipalId::parse("prn_root").expect("principal");
    let recovered = writer
        .recover_administrator(&namespace, &principal, loonfs_test_support::test_actor())
        .await
        .expect("recovery");
    assert_eq!(recovered.access_revision_no, AccessRevisionNo(2));
    commit_as(
        &writer,
        &namespace,
        subject("root", "prn_root"),
        create_directory("/docs"),
    )
    .await
    .expect("administrator again");
    let feed = writer
        .reader()
        .list_changes(&namespace, ChangeSeq(0), ListChangesOptions { limit: None })
        .await
        .expect("feed");
    let root_rows: Vec<_> = feed
        .changes
        .iter()
        .flat_map(|commit| &commit.events)
        .filter_map(|event| match event {
            FilesystemChange::AccessChanged {
                inode_id,
                access_revision_no,
                grants,
                ..
            } if *inode_id == ROOT_INODE_ID => Some((*access_revision_no, grants.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        root_rows.last(),
        Some(&(AccessRevisionNo(2), root_grants(true)))
    );
    let again = writer
        .recover_administrator(&namespace, &principal, loonfs_test_support::test_actor())
        .await
        .expect("second recovery");
    assert_eq!(again.access_revision_no, AccessRevisionNo(3));
}

#[tokio::test]
async fn a_revoked_administrator_cannot_delete_a_snapshot_through_the_former_writer() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let root = writer.as_subject(subject("root", "prn_root"));
    let now_ms = loonfs_core::time::current_time_ms().expect("clock");
    let snapshot = root
        .create_snapshot_with_quota(
            &namespace,
            CreateSnapshotOptions {
                name: "protected".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
            now_ms,
            10,
        )
        .await
        .expect("create snapshot");
    let snapshot_id = snapshot.checkpoint_id.into();
    let mut options = loonfs::PutFileOptions::new(loonfs_test_support::test_actor());
    options.commit.subject = Some(subject("root", "prn_root"));
    root.put_file_bytes(&namespace, "/file", b"private payload", options)
        .await
        .expect("publish after snapshot creation");
    let peer = FsWriter::builder_with_store(writer.object_store())
        .writer_id("peer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("peer");
    commit_as(
        &peer,
        &namespace,
        subject("root", "prn_root"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::new(BTreeMap::from([(
                PrincipalId::parse("prn_new_root").expect("principal"),
                AccessRights::from_iter([AccessRight::Admin]),
            )]))
            .expect("grants"),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
    )
    .await
    .expect("replace administrator");
    assert_eq!(
        root.delete_snapshot(&namespace, &snapshot_id)
            .await
            .expect_err("revoked administrator cannot delete the snapshot")
            .code(),
        ErrorCode::Forbidden
    );
    let reader = loonfs::FsReader::builder_with_store(writer.object_store())
        .build()
        .await
        .expect("fresh reader");
    let _snapshot = reader
        .pin_namespace_at_snapshot(&namespace, &snapshot_id)
        .await
        .expect("snapshot still exists");
    peer.shutdown().await.expect("peer shutdown");
    writer.shutdown().await.expect("old writer shutdown");
}
