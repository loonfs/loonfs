//! Namespace administration and recovery under ACL access.

use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{
    CreateNamespaceOptions, CreateSnapshotOptions, DeleteNamespaceOptions, ForkNamespaceOptions,
    ListChangesOptions, LoonFs, SnapshotPolicy, StatPathOptions, Writable,
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

async fn create_namespace() -> (tempfile::TempDir, LoonFs<Writable>, NamespaceId) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let writer =
        LoonFs::builder_with_store(Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")))
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
    let namespace_reader = writer.namespace(&namespace);
    let root = writer.as_subject(subject("root", "prn_root"));
    let member = writer.as_subject(subject("member", "team"));
    assert_eq!(
        namespace_reader.metadata().await.expect("namespace").access,
        access_mode()
    );
    let fork = namespace_id("fork");
    let fork_namespace = writer.namespace(&fork);
    let member_fork_namespace = member.namespace(&fork);
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
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let root_namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    let member_namespace_writer = member.open_namespace(&namespace).expect("open namespace");
    assert_eq!(
        fork_namespace.metadata().await.expect("fork").access,
        access_mode()
    );
    member_fork_namespace
        .get_path_entry("/", StatPathOptions::default())
        .await
        .expect("inherited member grant");
    let now_ms = loonfs_core::time::current_time_ms().expect("clock");
    let snapshot_options = CreateSnapshotOptions {
        name: "snapshot".to_owned(),
        expires_at_ms: now_ms + 60_000,
    };
    assert_eq!(
        member_namespace_writer
            .create_snapshot(
                snapshot_options.clone(),
                SnapshotPolicy::default().max_live_per_namespace
            )
            .await
            .expect_err("member snapshot")
            .code(),
        ErrorCode::Forbidden
    );
    let snapshot = root_namespace_writer
        .create_snapshot(
            snapshot_options,
            SnapshotPolicy::default().max_live_per_namespace,
        )
        .await
        .expect("root snapshot");
    assert_eq!(
        member_namespace_writer
            .delete_snapshot(&snapshot.checkpoint_id)
            .await
            .expect_err("member snapshot delete")
            .code(),
        ErrorCode::Forbidden
    );
    let delete_options = DeleteNamespaceOptions::default();
    assert_eq!(
        member_namespace_writer
            .delete_namespace(delete_options)
            .await
            .expect_err("member namespace delete")
            .code(),
        ErrorCode::Forbidden
    );
    namespace_writer
        .delete_namespace(delete_options)
        .await
        .expect("token holder delete");
}

async fn commit_as(
    writer: &LoonFs<Writable>,
    namespace: &NamespaceId,
    subject: Subject,
    operation: FilesystemOperation,
) -> Result<loonfs_api::Commit, loonfs::RuntimeError> {
    let namespace_writer = writer.open_namespace(namespace)?;
    namespace_writer
        .create_commit(
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
    let wrong_scope_namespace = writer
        .read_only()
        .as_subject(wrong_scope.clone())
        .namespace(&namespace);
    let error = wrong_scope_namespace
        .get_path_entry("/", StatPathOptions::default())
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
    let unrestricted_namespace = writer
        .read_only()
        .as_subject(wrong_scope.clone())
        .namespace(&unrestricted);
    unrestricted_namespace
        .get_path_entry("/", StatPathOptions::default())
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
    let namespace_reader = writer.namespace(&namespace);
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
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
    let recovered = namespace_writer
        .recover_administrator(&principal, loonfs_test_support::test_actor())
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
    let feed = namespace_reader
        .list_changes_page(ChangeSeq(0), ListChangesOptions { limit: None })
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
    let again = namespace_writer
        .recover_administrator(&principal, loonfs_test_support::test_actor())
        .await
        .expect("second recovery");
    assert_eq!(again.access_revision_no, AccessRevisionNo(3));
}

#[tokio::test]
async fn a_revoked_administrator_cannot_delete_a_snapshot_through_the_former_writer() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let root = writer.as_subject(subject("root", "prn_root"));
    let namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    let now_ms = loonfs_core::time::current_time_ms().expect("clock");
    let snapshot = namespace_writer
        .create_snapshot(
            CreateSnapshotOptions {
                name: "protected".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
            SnapshotPolicy::default().max_live_per_namespace,
        )
        .await
        .expect("create snapshot");
    let snapshot_id = snapshot.checkpoint_id;
    let options = loonfs::PutFileOptions::new(loonfs_test_support::test_actor());
    namespace_writer
        .put_file_bytes("/file", b"private payload", options)
        .await
        .expect("publish after snapshot creation");
    let peer = LoonFs::builder_with_store(writer.object_store())
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
        namespace_writer
            .delete_snapshot(&snapshot_id)
            .await
            .expect_err("revoked administrator cannot delete the snapshot")
            .code(),
        ErrorCode::Forbidden
    );
    let reader = loonfs::LoonFs::reader_with_store(writer.object_store())
        .build()
        .await
        .expect("fresh reader");
    let namespace_reader = reader.namespace(&namespace);
    let _view = namespace_reader
        .read_view_at_snapshot(&snapshot_id)
        .await
        .expect("snapshot still exists");
    peer.shutdown().await.expect("peer shutdown");
    writer.shutdown().await.expect("old writer shutdown");
}

#[tokio::test]
async fn a_scoped_writer_uses_its_subject_for_commits_and_upload_ownership() {
    let (_directory, writer, namespace) = create_namespace().await;
    let scoped = writer.as_subject(subject("root", "prn_root"));
    let scoped_namespace_writer = scoped.open_namespace(&namespace).expect("open namespace");
    scoped_namespace_writer
        .put_file_bytes(
            "/scoped",
            b"bytes",
            loonfs::PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("scoped commit");
    let upload = scoped_namespace_writer
        .create_upload()
        .await
        .expect("scoped upload");
    scoped_namespace_writer
        .get_upload(&upload.upload_id)
        .await
        .expect("same subject");
    let staged = scoped_namespace_writer
        .prepare_file_bytes(&vec![0; 128 * 1024])
        .await
        .expect("stage scoped content");
    scoped_namespace_writer
        .get_upload(staged.upload_id().expect("staged upload"))
        .await
        .expect("staged upload belongs to the scoped subject");
    let other = writer.as_subject(subject("other", "prn_root"));
    let other_namespace_writer = other.open_namespace(&namespace).expect("open namespace");
    assert_eq!(
        other_namespace_writer
            .get_upload(&upload.upload_id)
            .await
            .expect_err("another subject cannot read the session")
            .code(),
        ErrorCode::UploadNotFound
    );
}

async fn snapshot_after_administrator_change() -> (
    tempfile::TempDir,
    loonfs::LoonFs<loonfs::ReadOnly>,
    NamespaceId,
    loonfs_api::PinId,
    loonfs_api::ContentRef,
) {
    let (directory, writer, namespace) = create_namespace().await;
    let root = writer.as_subject(subject("root", "prn_root"));
    let namespace_reader = root.namespace(&namespace);
    let namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file_bytes(
            "/file",
            b"snapshot payload",
            loonfs::PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("seed snapshot content");
    let content = namespace_reader
        .get_path_entry("/file", StatPathOptions::default())
        .await
        .expect("file")
        .content_ref()
        .expect("content reference")
        .clone();
    let snapshot = namespace_writer
        .create_snapshot(
            CreateSnapshotOptions {
                name: "old-admin".to_owned(),
                expires_at_ms: loonfs_core::time::current_time_ms().expect("clock") + 60_000,
            },
            SnapshotPolicy::default().max_live_per_namespace,
        )
        .await
        .expect("snapshot");
    commit_as(
        &writer,
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
    // A new runtime rules out stale-cache permission checks. The old ACL is
    // available only through the deliberately historical snapshot context.
    let reader = loonfs::LoonFs::reader_with_store(writer.object_store())
        .build()
        .await
        .expect("fresh reader");
    writer.shutdown().await.expect("shutdown writer");
    (
        directory,
        reader,
        namespace,
        snapshot.checkpoint_id,
        content,
    )
}

#[tokio::test]
async fn snapshot_admin_reads_reject_a_revoked_administrator() {
    let (_directory, reader, namespace, snapshot_id, content) =
        snapshot_after_administrator_change().await;
    let revoked = reader.as_subject(subject("root", "prn_root"));
    let namespace_reader = revoked.namespace(&namespace);
    assert_eq!(
        namespace_reader
            .list_changes_page(ChangeSeq(0), ListChangesOptions::default())
            .await
            .expect_err("live feed rejects the old administrator")
            .code(),
        ErrorCode::Forbidden
    );
    let view = namespace_reader
        .read_view_at_snapshot(&snapshot_id)
        .await
        .expect("load historical view");
    assert_eq!(
        view.get_file_bytes("/file")
            .await
            .expect_err("ordinary snapshot reads use current authority")
            .code(),
        ErrorCode::PathNotFound
    );
    let changes = view
        .list_changes_page(
            ChangeSeq(0),
            ListChangesOptions {
                limit: Some(loonfs_api::EffectiveLimit::new(
                    std::num::NonZeroU32::new(10).expect("limit"),
                )),
            },
        )
        .await;
    let bytes = view.read_content_ref(&content, 100).await;
    assert!(
        matches!(&changes, Err(error) if error.code() == ErrorCode::Forbidden)
            && matches!(&bytes, Err(error) if error.code() == ErrorCode::Forbidden),
        "revoked snapshot administrator: changes={changes:?}, bytes={bytes:?}"
    );
}

#[tokio::test]
async fn snapshot_admin_reads_accept_the_current_administrator() {
    let (_directory, reader, namespace, snapshot_id, content) =
        snapshot_after_administrator_change().await;
    let namespace_reader = reader
        .as_subject(subject("new-root", "prn_new_root"))
        .namespace(&namespace);
    let view = namespace_reader
        .read_view_at_snapshot(&snapshot_id)
        .await
        .expect("load historical view");
    let changes = view
        .list_changes_page(
            ChangeSeq(0),
            ListChangesOptions {
                limit: Some(loonfs_api::EffectiveLimit::new(
                    std::num::NonZeroU32::new(10).expect("limit"),
                )),
            },
        )
        .await
        .expect("current administrator can read historical changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        view.read_content_ref(&content, 100)
            .await
            .expect("current administrator can read historical content"),
        b"snapshot payload"
    );
}
