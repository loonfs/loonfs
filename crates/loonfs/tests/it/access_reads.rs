//! Subject reads observe access changes through the runtime cache.

use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{CreateNamespaceOptions, DestinationBehavior, LoonFs, StatPathOptions};
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ErrorCode, NamespaceAccess,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
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
        .writer_id("access-reads")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("access-reads");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", AccessRight::Admin),
                },
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    // Keep the reader's cache independent so a write cannot invalidate it.
    let reader = LoonFs::reader_with_store(store)
        .build()
        .await
        .expect("reader")
        .as_subject(subject("viewer"));
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
            .create_commit(
                CommitRequest::single(
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
    namespace
        .get_path_entry("/team", StatPathOptions::default())
        .await
        .expect("warm read");
    for byte in *b"ab" {
        let bytes = vec![byte; content_size];
        let prepared = namespace_writer
            .prepare_file_bytes(&bytes)
            .await
            .expect("prepare file");
        namespace_writer
            .commit_prepared(
                CommitRequest::single(
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
                .get_file_bytes("/team/file")
                .await
                .expect("read with changed or unchanged metadata");
            assert_eq!(read.bytes, bytes);
        }
        assert_eq!(
            namespace
                .as_subject(subject("stranger"))
                .get_file_bytes("/team/file")
                .await
                .expect_err("a shared cache does not grant another subject access")
                .code(),
            ErrorCode::PathNotFound
        );
    }
    let inode_id = namespace
        .get_path_entry("/team/file", StatPathOptions::default())
        .await
        .expect("shared file before revocation")
        .inode_id;
    namespace_writer
        .create_commit(
            CommitRequest::single(
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
            .get_file_bytes("/team/file")
            .await
            .expect_err("cached content cannot bypass revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        namespace
            .get_path_entry("/team", StatPathOptions::default())
            .await
            .expect_err("revoked")
            .code(),
        ErrorCode::PathNotFound
    );
    let states = namespace
        .resolve_current_files(&[inode_id])
        .await
        .expect("resolve revoked file");
    assert_eq!(states.len(), 1);
    let state = &states[0];
    assert_eq!(state.inode_id, inode_id);
    assert!(!state.visible);
    assert!(!state.readable);
    assert_eq!(state.current_revision_no, None);
    assert_eq!(state.current_path, None);
}

#[tokio::test]
async fn a_former_writer_sees_revocation_on_its_first_read_after_publication() {
    check_former_writer_read(false).await;
}

#[tokio::test]
async fn a_former_writer_sees_revocation_after_a_warm_read_before_handoff() {
    check_former_writer_read(true).await;
}

async fn check_former_writer_read(warm_before_handoff: bool) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let old_writer = LoonFs::builder_with_store(store.clone())
        .writer_id("old-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("old writer");
    let namespace_id = namespace_id("access-handoff");
    old_writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", AccessRight::Admin),
                },
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("namespace");
    let old_namespace_writer = old_writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let options = loonfs::PutFileOptions::new(loonfs_test_support::test_actor());
    old_namespace_writer
        .as_subject(subject("prn_root"))
        .put_file_bytes("/team/file", b"private payload", options)
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
        .create_commit(
            CommitRequest::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                access(grants("viewer", AccessRight::Read)),
            )
            .with_subject(subject("prn_root")),
        )
        .await
        .expect("grant read access");
    let reader = old_writer.read_only().as_subject(subject("viewer"));
    let namespace = reader.namespace(&namespace_id);
    if warm_before_handoff {
        assert_eq!(
            namespace
                .get_file_bytes("/team/file")
                .await
                .expect("warm read")
                .bytes,
            b"private payload"
        );
    }
    let peer = LoonFs::builder_with_store(store)
        .writer_id("peer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("peer");
    let peer_namespace_writer = peer.open_namespace(&namespace_id).expect("open namespace");
    peer_namespace_writer
        .create_commit(
            CommitRequest::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                access(AccessGrants::default()),
            )
            .with_subject(subject("prn_root")),
        )
        .await
        .expect("revoke read access");
    assert_eq!(
        namespace
            .get_file_bytes("/team/file")
            .await
            .expect_err("first read observes revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    peer.shutdown().await.expect("peer shutdown");
    old_writer.shutdown().await.expect("old writer shutdown");
}
