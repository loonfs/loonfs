//! Subject reads observe access changes through the runtime cache.

use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{CreateNamespaceOptions, CurrentFile, DestinationBehavior, LoonFs};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ErrorCode, NamespaceAccess,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(std::collections::BTreeSet::from([PrincipalId::parse(
            principal,
        )
        .expect("principal")]))
        .expect("principals"),
    }
}

fn grants(principal: &str, right: AccessRight) -> AccessGrants {
    AccessGrants::new(std::collections::BTreeMap::from([(
        PrincipalId::parse(principal).expect("principal"),
        AccessRights::from_iter([right]),
    )]))
    .expect("grants")
}

#[tokio::test]
async fn a_warmed_reader_sees_a_revocation_on_its_next_read() {
    for content_size in [0, 5, 64 * 1024, 64 * 1024 + 1] {
        check_buffered_read_access(content_size).await;
    }
}

async fn check_buffered_read_access(content_size: usize) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("access-reads"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("access-reads");
    writer
        .create_namespace_with_options(
            &namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", AccessRight::Admin),
                },
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    // Keep the reader's cache independent so a write cannot invalidate it.
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("reader")
        .with_subject(subject("viewer"));
    let namespace = reader.namespace(&namespace_id);
    for operation in [
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/team").expect("path"),
            parents: false,
        },
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::parse("/team").expect("path"),
            boundary: false,
            grants: grants("viewer", AccessRight::Read),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
    ] {
        namespace_writer
            .commit(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                )
                .with_subject(subject("prn_root")),
            )
            .await
            .expect("seed");
    }
    namespace.stat("/team").await.expect("warm read");
    for byte in *b"ab" {
        let bytes = vec![byte; content_size];
        let prepared = namespace_writer
            .prepare_content(&bytes)
            .await
            .expect("prepare file");
        namespace_writer
            .commit_prepared(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/team/file").expect("path"),
                        content_ref: Some(prepared.content_ref().clone()),
                        inline_content: None,
                        behavior: DestinationBehavior::Replace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                )
                .with_subject(subject("prn_root")),
                vec![prepared],
            )
            .await
            .expect("publish file");
        for _ in 0..2 {
            let read = namespace
                .read_file("/team/file")
                .await
                .expect("read with changed or unchanged metadata");
            assert_eq!(read.bytes, bytes);
        }
        assert_eq!(
            namespace
                .with_subject(subject("stranger"))
                .read_file("/team/file")
                .await
                .expect_err("a shared cache does not grant another subject access")
                .code(),
            ErrorCode::PathNotFound
        );
    }
    let inode_id = namespace
        .stat("/team/file")
        .await
        .expect("shared file before revocation")
        .inode_id;
    namespace_writer
        .commit(
            CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::UpdateAccess {
                    path: AbsolutePath::parse("/team").expect("path"),
                    boundary: false,
                    grants: AccessGrants::default(),
                    expected_inode_id: None,
                    expected_access_revision_no: None,
                },
            )
            .with_subject(subject("prn_root")),
        )
        .await
        .expect("revoke");
    assert_eq!(
        namespace
            .read_file("/team/file")
            .await
            .expect_err("cached content cannot bypass revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        namespace.stat("/team").await.expect_err("revoked").code(),
        ErrorCode::PathNotFound
    );
    let states = namespace
        .stat_current_files_by_inode(&[inode_id])
        .await
        .expect("resolve revoked file");
    assert_eq!(states.len(), 1);
    let state = &states[0];
    assert_eq!(state.inode_id, inode_id);
    assert_eq!(state.file, CurrentFile::Hidden { readable: false });
}

/// The manifest check interval of the former writer's runtime in
/// [`check_former_writer_read`].
#[derive(Debug, Clone, Copy)]
enum CheckInterval {
    /// An interval of 0: every read checks for a newer manifest.
    Zero,
    /// The default interval of one second. The test moves the runtime's
    /// clock past it before the read.
    Default,
}

/// Checks that a former writer's first read after another writer revokes
/// access sees the revocation when its runtime checks for a newer manifest
/// on every read.
///
/// Reads in a runtime with the namespace's writer session start from the
/// session's position, and only the manifest check finds another writer's
/// takeover. With an interval of 0, every read checks, so the revocation is
/// visible on the first read.
#[tokio::test]
async fn a_former_writer_sees_revocation_on_its_first_read_with_a_zero_interval() {
    check_former_writer_read(false, CheckInterval::Zero).await;
}

/// Checks the same guarantee as
/// [`a_former_writer_sees_revocation_on_its_first_read_with_a_zero_interval`]
/// when the former writer's runtime read the file before the handoff.
#[tokio::test]
async fn a_former_writer_sees_revocation_after_a_warm_read_with_a_zero_interval() {
    check_former_writer_read(true, CheckInterval::Zero).await;
}

/// Checks that a former writer sees another writer's revocation within one
/// manifest check interval under the default interval: the first read after
/// the interval passes finds the new writer's claim manifest and doesn't
/// return the file.
#[tokio::test]
async fn a_former_writer_sees_revocation_within_one_default_interval() {
    for warm_before_handoff in [false, true] {
        check_former_writer_read(warm_before_handoff, CheckInterval::Default).await;
    }
}

async fn check_former_writer_read(warm_before_handoff: bool, interval: CheckInterval) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000_000));
    let builder = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("old-writer"))
        .min_publish_interval_ms(0)
        .monotonic_timer(clock.clone());
    let old_writer = match interval {
        CheckInterval::Zero => builder.manifest_revalidation_interval_ms(0),
        CheckInterval::Default => builder,
    }
    .build()
    .await
    .expect("old writer");
    let namespace_id = namespace_id("access-handoff");
    old_writer
        .create_namespace_with_options(
            &namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", AccessRight::Admin),
                },
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
    let old_namespace_writer = old_writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let options = loonfs::PutFileOptions::default();
    old_namespace_writer
        .with_subject(subject("prn_root"))
        .put_file_with_options(
            "/team/file",
            b"private payload",
            &loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("publish file");
    let access = |grants| FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse("/team").expect("path"),
        boundary: true,
        grants,
        expected_inode_id: None,
        expected_access_revision_no: None,
    };
    old_namespace_writer
        .commit(
            CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                access(grants("viewer", AccessRight::Read)),
            )
            .with_subject(subject("prn_root")),
        )
        .await
        .expect("grant read access");
    let reader = old_writer.read_only().with_subject(subject("viewer"));
    let namespace = reader.namespace(&namespace_id);
    if warm_before_handoff {
        assert_eq!(
            namespace
                .read_file("/team/file")
                .await
                .expect("warm read")
                .bytes,
            b"private payload"
        );
    }
    let peer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("peer"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("peer");
    let peer_namespace_writer = peer.open_namespace(&namespace_id).expect("open namespace");
    peer_namespace_writer
        .commit(
            CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                access(AccessGrants::default()),
            )
            .with_subject(subject("prn_root")),
        )
        .await
        .expect("revoke read access");
    if let CheckInterval::Default = interval {
        clock.advance_ms(1_000);
    }
    assert_eq!(
        namespace
            .read_file("/team/file")
            .await
            .expect_err("the read observes the revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    peer.shutdown().await.expect("peer shutdown");
    old_writer.shutdown().await.expect("old writer shutdown");
}
