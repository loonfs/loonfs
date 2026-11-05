//! Subject reads observe access changes through the runtime cache, and a stat
//! batch hides each inode that the subject can't read.

use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{
    Consistency, CreateNamespaceOptions, CurrentFile, CurrentFileState, DestinationBehavior,
    InodeId, LoonFs, NamespaceId, ReadAt, ReadFileOptions, RevisionNo, Writable,
};
use loonfs_core::test_support::STORE_READS_IN_FLIGHT;
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
        AccessRights::from(right),
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
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
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
    // A maximum staleness of 0 confirms the head on every read, as a
    // `strong` read does, so the next read after a change sees it.
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .config(loonfs::RuntimeConfig {
            max_staleness_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
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
                        content: loonfs_types::FileContent::Ref {
                            content_ref: prepared.content_ref().clone(),
                            source_namespace_id: None,
                        },
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

/// Checks that a warmed reader's eventual reads see a revocation on the first
/// read after its maximum staleness passes, and not before.
///
/// An eventual read within its maximum staleness reads at the head that the reader
/// confirmed last, so it still returns the file after the revocation. The
/// first eventual read after the maximum staleness passes confirms the head and finds the
/// revocation.
#[tokio::test]
async fn a_warmed_reader_sees_a_revocation_on_its_first_eventual_read_after_its_maximum_staleness()
{
    for content_size in [0, 5, 64 * 1024, 64 * 1024 + 1] {
        check_eventual_read_access(content_size).await;
    }
}

async fn check_eventual_read_access(content_size: usize) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("eventual-access"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("eventual-access");
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
    let bytes = vec![b'a'; content_size];
    namespace_writer
        .with_subject(subject("prn_root"))
        .put_file_with_options(
            "/team/file",
            &bytes,
            &loonfs_test_support::test_actor(),
            &loonfs::PutFileOptions::default(),
        )
        .await
        .expect("publish file");
    let access = |grants| {
        CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::UpdateAccess {
                path: AbsolutePath::parse("/team").expect("path"),
                boundary: true,
                grants,
                expected_inode_id: None,
                expected_access_revision_no: None,
            },
        )
        .with_subject(subject("prn_root"))
    };
    namespace_writer
        .commit(access(grants("viewer", AccessRight::Read)))
        .await
        .expect("grant read access");
    // A separate runtime, so the reader confirms the head from the store.
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000_000));
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .monotonic_timer(clock.clone())
        .build()
        .await
        .expect("reader")
        .with_subject(subject("viewer"));
    let namespace = reader.namespace(&namespace_id);
    let eventual = ReadFileOptions {
        at: ReadAt::Live(Some(Consistency::Eventual {
            max_staleness_ms: None,
        })),
    };
    let read = namespace
        .read_file_with_options("/team/file", &eventual)
        .await
        .expect("warm read");
    assert_eq!(read.bytes, bytes);
    namespace_writer
        .commit(access(AccessGrants::default()))
        .await
        .expect("revoke read access");
    let read = namespace
        .read_file_with_options("/team/file", &eventual)
        .await
        .expect(
            "a read within the maximum staleness reads the head confirmed before the revocation",
        );
    assert_eq!(read.bytes, bytes);
    clock.advance_ms(1_000);
    assert_eq!(
        namespace
            .read_file_with_options("/team/file", &eventual)
            .await
            .expect_err("the first read after the maximum staleness observes the revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    writer.shutdown().await.expect("writer shutdown");
}

/// Checks that a former writer's first strong read after another writer
/// revokes access sees the revocation, whether or not its runtime read the
/// file before the handoff.
///
/// Reads in a runtime with the namespace's writer session start from the
/// session's position, and the session's check of the position's manifest
/// finds another writer's takeover. A strong read waits for that check, so
/// the revocation is visible on the first read.
#[tokio::test]
async fn a_former_writer_sees_revocation_on_its_first_strong_read() {
    for warm_before_handoff in [false, true] {
        check_former_writer_read(warm_before_handoff, Consistency::Strong).await;
    }
}

/// Checks that a former writer sees another writer's revocation once an
/// eventual read's maximum staleness passes: the first eventual read after
/// that waits for the session's check, finds the new writer's claim
/// manifest, and doesn't return the file.
#[tokio::test]
async fn a_former_writer_sees_revocation_past_the_maximum_staleness() {
    for warm_before_handoff in [false, true] {
        let eventual = Consistency::Eventual {
            max_staleness_ms: None,
        };
        check_former_writer_read(warm_before_handoff, eventual).await;
    }
}

async fn check_former_writer_read(warm_before_handoff: bool, consistency: Consistency) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000_000));
    let old_writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("old-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .monotonic_timer(clock.clone())
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
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
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
    if let Consistency::Eventual { .. } = consistency {
        clock.advance_ms(1_000);
    }
    assert_eq!(
        namespace
            .read_file_with_options(
                "/team/file",
                &ReadFileOptions {
                    at: ReadAt::Live(Some(consistency)),
                },
            )
            .await
            .expect_err("the read observes the revocation")
            .code(),
        ErrorCode::PathNotFound
    );
    peer.shutdown().await.expect("peer shutdown");
    old_writer.shutdown().await.expect("old writer shutdown");
}

/// Checks that a stat batch several read windows long returns each inode's
/// state in its request position: a file the subject can read, an inode ID
/// that doesn't exist, and a file that only an administrator can read.
#[tokio::test]
async fn a_stat_batch_returns_readable_unknown_and_unreadable_inodes_in_request_order() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("stat-batch-access"))
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("stat-batch-access");
    // Each index adds three requests, so the batch is three read windows
    // long and later requests start while earlier ones are in flight.
    let files = STORE_READS_IN_FLIGHT;
    publish_team_and_private_files(&writer, &namespace_id, files).await;
    let administrator = writer
        .read_only()
        .with_subject(subject("prn_root"))
        .namespace(&namespace_id);
    let viewer = writer
        .read_only()
        .with_subject(subject("viewer"))
        .namespace(&namespace_id);
    let visible = |inode_id, path: String| CurrentFileState {
        inode_id,
        file: CurrentFile::Visible {
            path: AbsolutePath::parse(path).expect("path"),
            revision_no: Some(RevisionNo(1)),
        },
    };
    let hidden = |inode_id| CurrentFileState {
        inode_id,
        file: CurrentFile::Hidden { readable: false },
    };
    let mut requested = Vec::new();
    let mut for_viewer = Vec::new();
    let mut for_administrator = Vec::new();
    for index in 0..files {
        let (team, private) = (team_file(index), private_file(index));
        let readable = administrator.stat(&team).await.expect("stat").inode_id;
        let unreadable = administrator.stat(&private).await.expect("stat").inode_id;
        let unknown = InodeId(1_000_000 + index as u128);
        requested.extend([readable, unknown, unreadable]);
        for_viewer.extend([
            visible(readable, team.clone()),
            hidden(unknown),
            hidden(unreadable),
        ]);
        for_administrator.extend([
            visible(readable, team),
            hidden(unknown),
            visible(unreadable, private),
        ]);
    }
    assert_eq!(
        viewer
            .stat_current_files_by_inode(&requested)
            .await
            .expect("the viewer's batch"),
        for_viewer
    );
    assert_eq!(
        administrator
            .stat_current_files_by_inode(&requested)
            .await
            .expect("the administrator's batch"),
        for_administrator
    );
}

#[tokio::test]
async fn listing_a_checkpoints_files_requires_an_administrator() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id(
            "checkpoint-files-access",
        ))
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("checkpoint-files-access");
    publish_team_and_private_files(&writer, &namespace_id, 1).await;
    let checkpoint = writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .with_subject(subject("prn_root"))
        .create_snapshot("checkpoint")
        .await
        .expect("create checkpoint");
    let list = |principal: &str| {
        writer
            .read_only()
            .with_subject(subject(principal))
            .namespace(&namespace_id)
            .list_snapshot_files(&checkpoint.snapshot_id)
            .page()
    };

    let error = list("viewer")
        .await
        .expect_err("a reader of /team isn't an administrator");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    let page = list("prn_root").await.expect("the administrator's listing");
    assert_eq!(page.files.len(), 2);
}

fn team_file(index: usize) -> String {
    format!("/team/file-{index}")
}

fn private_file(index: usize) -> String {
    format!("/private/file-{index}")
}

/// Creates an ACL namespace and publishes `files` files under `/team`, which
/// `viewer` can read, and as many under `/private`, which only `prn_root`
/// can read.
async fn publish_team_and_private_files(
    writer: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    files: usize,
) {
    writer
        .create_namespace_with_options(
            namespace_id,
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
        .open_namespace(namespace_id)
        .expect("open namespace")
        .with_subject(subject("prn_root"));
    let prepared = namespace_writer
        .prepare_content(b"body")
        .await
        .expect("prepare content");
    let put = |path: String| FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content: loonfs_types::FileContent::Ref {
            content_ref: prepared.content_ref().clone(),
            source_namespace_id: None,
        },
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let mut operations: Vec<_> = (0..files)
        .flat_map(|index| [put(team_file(index)), put(private_file(index))])
        .collect();
    operations.push(FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse("/team").expect("path"),
        boundary: false,
        grants: grants("viewer", AccessRight::Read),
        expected_inode_id: None,
        expected_access_revision_no: None,
    });
    let mut request = CommitIntent::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        operations.remove(0),
    );
    request.operations.extend(operations);
    namespace_writer
        .commit_prepared(request, vec![prepared.clone()])
        .await
        .expect("publish the files and the grant");
}
