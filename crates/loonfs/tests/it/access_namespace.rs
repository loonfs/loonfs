//! Namespace administration, and setting administrators in the service context.

use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{CreateSnapshotOptions, LoonFs, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::FilesystemEvent;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, AccessRights, CommitId, CommitSeq,
    ErrorCode, NamespaceId, PrincipalId, PrincipalSet, Subject, ROOT_INODE_ID,
};
use std::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
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
            AccessRight::Admin.into(),
        );
    }
    AccessGrants::new(grants).expect("grants")
}

async fn create_namespace() -> (tempfile::TempDir, LoonFs<Writable>, NamespaceId) {
    create_namespace_with(loonfs::RuntimeConfig::default()).await
}

/// Creates a namespace whose root row makes `prn_root` an administrator,
/// from a writer runtime with `config` and no minimum interval between its
/// publications.
async fn create_namespace_with(
    config: loonfs::RuntimeConfig,
) -> (tempfile::TempDir, LoonFs<Writable>, NamespaceId) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let writer = LoonFs::builder_with_store(
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")),
        loonfs::Context::Service,
    )
    .writer_id(loonfs_test_support::ids::writer_id("access-namespace"))
    .config(loonfs::RuntimeConfig {
        min_publish_interval_ms: 0,
        ..config
    })
    .build()
    .await
    .expect("writer");
    let namespace_id = namespace_id("access-namespace");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    loonfs::test_support::set_root_access(&writer, &namespace_id, root_grants(true)).await;
    (temp_dir, writer, namespace_id)
}

#[tokio::test]
async fn namespace_operations_need_an_administrator_or_the_service_context() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let root = writer.with_context(subject("prn_root").into());
    let member = writer.with_context(subject("team").into());
    let fork = namespace_id("fork");
    let member_fork_namespace = member.namespace(&fork);
    assert_eq!(
        member
            .fork_namespace(&namespace, &fork, &loonfs_test_support::test_actor(),)
            .await
            .expect_err("member fork")
            .code(),
        ErrorCode::Forbidden
    );
    root.fork_namespace(&namespace, &fork, &loonfs_test_support::test_actor())
        .await
        .expect("root fork");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let root_namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    let member_namespace_writer = member.open_namespace(&namespace).expect("open namespace");
    member_fork_namespace
        .stat("/")
        .await
        .expect("inherited member grant");
    assert_eq!(
        member_namespace_writer
            .create_snapshot_with_options(&CreateSnapshotOptions {
                name: Some("snapshot".to_owned()),
                ttl_ms: std::num::NonZeroU64::new(60_000),
            })
            .await
            .expect_err("member snapshot")
            .code(),
        ErrorCode::Forbidden
    );
    let snapshot = root_namespace_writer
        .create_snapshot_with_options(&CreateSnapshotOptions {
            name: Some("snapshot".to_owned()),
            ttl_ms: std::num::NonZeroU64::new(60_000),
        })
        .await
        .expect("root snapshot");
    assert_eq!(
        member_namespace_writer
            .delete_snapshot(&snapshot.snapshot_id)
            .await
            .expect_err("member snapshot delete")
            .code(),
        ErrorCode::Forbidden
    );
    assert_eq!(
        member_namespace_writer
            .delete()
            .await
            .expect_err("member namespace delete")
            .code(),
        ErrorCode::Forbidden
    );
    namespace_writer
        .delete()
        .await
        .expect("service context delete");
}

async fn commit_as(
    writer: &LoonFs<Writable>,
    namespace: &NamespaceId,
    subject: Subject,
    operation: FilesystemOperation,
) -> Result<loonfs_types::Commit, loonfs::Error> {
    let namespace_writer = writer.open_namespace(namespace)?;
    namespace_writer
        .commit(
            CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                operation,
            )
            .with_subject(subject),
        )
        .await
}

fn create_directory(path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(path).expect("path"),
        parents: false,
    }
}

/// Checks that an `update_access` on the root in the service context sets the
/// namespace's administrators, after the last administrator removed itself,
/// and that a principal it names then has `admin`.
#[tokio::test]
async fn the_service_context_sets_the_administrators() {
    let (_temp_dir, writer, namespace) = create_namespace().await;
    let namespace_reader = writer.namespace(&namespace);
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    commit_as(
        &writer,
        &namespace,
        subject("prn_root"),
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
            subject("prn_root"),
            create_directory("/docs")
        )
        .await
        .expect_err("no rights left")
        .code(),
        ErrorCode::PathNotFound
    );
    let restored = namespace_writer
        .update_access(
            "/",
            &loonfs_test_support::test_actor(),
            loonfs::AccessState {
                boundary: false,
                grants: root_grants(true),
            },
        )
        .await
        .expect("the service context sets the administrators");
    assert_eq!(restored.committed_seq, CommitSeq(3));
    commit_as(
        &writer,
        &namespace,
        subject("prn_root"),
        create_directory("/docs"),
    )
    .await
    .expect("administrator again");
    writer
        .with_context(subject("prn_root").into())
        .namespace(&namespace)
        .list_changes(CommitSeq(0))
        .page()
        .await
        .expect("an administrator reads the feed");
    let feed = namespace_reader
        .list_changes(CommitSeq(0))
        .page()
        .await
        .expect("feed");
    let root_rows: Vec<_> = feed
        .changes
        .iter()
        .flat_map(|commit| &commit.events)
        .filter_map(|event| match event {
            FilesystemEvent::AccessChanged {
                inode_id,
                access_revision_no,
                grants,
                ..
            } if *inode_id == ROOT_INODE_ID => Some((*access_revision_no, grants.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        root_rows,
        [
            (AccessRevisionNo(1), root_grants(true)),
            (AccessRevisionNo(2), root_grants(false)),
            (AccessRevisionNo(3), root_grants(true)),
        ]
    );
}

/// Checks that an administrator whom another writer revoked can't delete a
/// snapshot through the former writer's runtime, when that runtime checks
/// for a newer manifest on every read.
///
/// The deletion authorizes against the head that the former writer's reads
/// start from, its session's position. Only the manifest check finds the
/// new writer's claim, so with the default interval the revocation applies
/// in the former writer's runtime within one manifest check interval, and
/// with an interval of 0 it applies at once.
#[tokio::test]
async fn a_revoked_administrator_cannot_delete_a_snapshot_through_the_former_writer() {
    let (_temp_dir, writer, namespace) = create_namespace_with(loonfs::RuntimeConfig {
        manifest_revalidation_interval_ms: 0,
        ..loonfs::RuntimeConfig::default()
    })
    .await;
    let root = writer.with_context(subject("prn_root").into());
    let namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    let snapshot = namespace_writer
        .create_snapshot_with_options(&CreateSnapshotOptions {
            name: Some("protected".to_owned()),
            ttl_ms: std::num::NonZeroU64::new(60_000),
        })
        .await
        .expect("create snapshot");
    let snapshot_id = snapshot.snapshot_id;
    let options = loonfs::PutFileOptions::default();
    namespace_writer
        .put_file_with_options(
            "/file",
            b"private payload",
            &loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("publish after snapshot creation");
    let peer = LoonFs::builder_with_store(writer.object_store(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("peer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("peer");
    commit_as(
        &peer,
        &namespace,
        subject("prn_root"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::new(BTreeMap::from([(
                PrincipalId::parse("prn_new_root").expect("principal"),
                AccessRight::Admin.into(),
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
    let reader =
        loonfs::LoonFs::builder_with_store(writer.object_store(), loonfs::Context::Service)
            .read_only()
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
async fn a_scoped_writer_uses_its_subject_for_commits_and_the_actor_owns_its_uploads() {
    let (_directory, writer, namespace) = create_namespace().await;
    let scoped = writer.with_context(subject("prn_root").into());
    let scoped_namespace_writer = scoped.open_namespace(&namespace).expect("open namespace");
    scoped_namespace_writer
        .put_file("/scoped", b"bytes", &loonfs_test_support::test_actor())
        .await
        .expect("scoped commit");
    let upload = scoped_namespace_writer
        .create_upload(&loonfs_test_support::test_actor())
        .await
        .expect("scoped upload");
    scoped_namespace_writer
        .get_upload(&upload.upload_id, &loonfs_test_support::test_actor())
        .await
        .expect("same actor");
    let staged = scoped_namespace_writer
        .prepare_content(&vec![0; 128 * 1024], &loonfs_test_support::test_actor())
        .await
        .expect("stage scoped content");
    scoped_namespace_writer
        .get_upload(
            staged.upload_id().expect("staged upload"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("staged upload belongs to the actor");
    assert_eq!(
        scoped_namespace_writer
            .get_upload(
                &upload.upload_id,
                &loonfs_types::ActorId::parse("usr_other").expect("actor id"),
            )
            .await
            .expect_err("another actor cannot read the session")
            .code(),
        ErrorCode::UploadNotFound
    );
}

async fn snapshot_after_administrator_change() -> (
    tempfile::TempDir,
    loonfs::LoonFs<loonfs::ReadOnly>,
    NamespaceId,
    loonfs_types::SnapshotId,
    loonfs_types::ContentRef,
) {
    let (directory, writer, namespace) = create_namespace().await;
    let root = writer.with_context(subject("prn_root").into());
    let namespace_reader = root.namespace(&namespace);
    let namespace_writer = root.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file(
            "/file",
            b"snapshot payload",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("seed snapshot content");
    let content = namespace_reader
        .stat("/file")
        .await
        .expect("file")
        .content_ref()
        .expect("content reference")
        .clone();
    let snapshot = namespace_writer
        .create_snapshot_with_options(&CreateSnapshotOptions {
            name: Some("old-admin".to_owned()),
            ttl_ms: std::num::NonZeroU64::new(60_000),
        })
        .await
        .expect("snapshot");
    commit_as(
        &writer,
        &namespace,
        subject("prn_root"),
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::root(),
            boundary: false,
            grants: AccessGrants::new(BTreeMap::from([(
                PrincipalId::parse("prn_new_root").expect("principal"),
                AccessRight::Admin.into(),
            )]))
            .expect("grants"),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
    )
    .await
    .expect("replace administrator");
    // A new runtime prevents stale-cache permission checks. The old ACL is
    // available only through the deliberately historical snapshot context.
    let reader =
        loonfs::LoonFs::builder_with_store(writer.object_store(), loonfs::Context::Service)
            .read_only()
            .build()
            .await
            .expect("fresh reader");
    writer.shutdown().await.expect("shutdown writer");
    (directory, reader, namespace, snapshot.snapshot_id, content)
}

#[tokio::test]
async fn snapshot_admin_reads_reject_a_revoked_administrator() {
    let (_directory, reader, namespace, snapshot_id, content) =
        snapshot_after_administrator_change().await;
    let revoked = reader.with_context(subject("prn_root").into());
    let namespace_reader = revoked.namespace(&namespace);
    assert_eq!(
        namespace_reader
            .list_changes(CommitSeq(0))
            .page()
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
        view.read_file("/file")
            .await
            .expect_err("ordinary snapshot reads use current authority")
            .code(),
        ErrorCode::PathNotFound
    );
    let changes = view
        .list_changes(CommitSeq(0))
        .page_limit(loonfs_types::EffectiveLimit::new(
            std::num::NonZeroU32::new(10).expect("limit"),
        ))
        .page()
        .await;
    let bytes = view.read_content(&content, 100).await;
    assert_matches!(
        &changes,
        Err(error) if error.code() == ErrorCode::Forbidden,
        "revoked snapshot administrator: changes={changes:?}, bytes={bytes:?}"
    );
    assert_matches!(
        &bytes,
        Err(error) if error.code() == ErrorCode::Forbidden,
        "revoked snapshot administrator: changes={changes:?}, bytes={bytes:?}"
    );
}

#[tokio::test]
async fn snapshot_admin_reads_accept_the_current_administrator() {
    let (_directory, reader, namespace, snapshot_id, content) =
        snapshot_after_administrator_change().await;
    let namespace_reader = reader
        .with_context(subject("prn_new_root").into())
        .namespace(&namespace);
    let view = namespace_reader
        .read_view_at_snapshot(&snapshot_id)
        .await
        .expect("load historical view");
    let changes = view
        .list_changes(CommitSeq(0))
        .page_limit(loonfs_types::EffectiveLimit::new(
            std::num::NonZeroU32::new(10).expect("limit"),
        ))
        .page()
        .await
        .expect("current administrator can read historical changes");
    // The root access row's commit and the file's.
    assert_eq!(changes.changes.len(), 2);
    assert_eq!(
        view.read_content(&content, 100)
            .await
            .expect("current administrator can read historical content"),
        b"snapshot payload"
    );
}
