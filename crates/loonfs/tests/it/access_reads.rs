//! Subject reads observe access changes through the runtime cache.

use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{CreateNamespaceOptions, DestinationBehavior, FsReader, FsWriter, StatPathOptions};
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ErrorCode, NamespaceAccess,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
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
    let writer = FsWriter::builder_with_store(store.clone())
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
    // Keep the reader's cache independent so a write cannot invalidate it.
    let reader = FsReader::builder_with_store(store)
        .build()
        .await
        .expect("reader")
        .as_subject(subject("viewer"));
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
        writer
            .create_commit(
                &namespace_id,
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
    reader
        .get_path_entry(&namespace_id, "/team", StatPathOptions::default())
        .await
        .expect("warm read");
    for byte in [b'a', b'b'] {
        let bytes = vec![byte; content_size];
        let prepared = writer
            .prepare_file_bytes(&namespace_id, &bytes)
            .await
            .expect("prepare file");
        writer
            .commit_prepared(
                &namespace_id,
                CommitRequest::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/team/file").expect("path"),
                        content_ref: prepared.content_ref().clone(),
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
            let read = reader
                .get_file_bytes(&namespace_id, "/team/file")
                .await
                .expect("read with changed or unchanged metadata");
            assert_eq!(read.bytes, bytes);
        }
        assert_eq!(
            reader
                .as_subject(subject("stranger"))
                .get_file_bytes(&namespace_id, "/team/file")
                .await
                .expect_err("a shared cache does not grant another subject access")
                .code(),
            ErrorCode::PathNotFound
        );
    }
    writer
        .create_commit(
            &namespace_id,
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
        reader
            .get_file_bytes(&namespace_id, "/team/file")
            .await
            .expect_err("cached content cannot bypass revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        reader
            .get_path_entry(&namespace_id, "/team", StatPathOptions::default())
            .await
            .expect_err("revoked")
            .code(),
        ErrorCode::PathNotFound
    );
}
