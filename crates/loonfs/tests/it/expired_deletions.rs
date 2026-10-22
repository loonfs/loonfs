//! Reads by inode ID of a subtree whose deletion the writer expired, and the
//! trash listing of a fork.

use loonfs::{
    AccessState, ChangeSeq, CommitId, CreateNamespaceOptions, CreateSnapshotOptions,
    DeleteDirectoryBehavior, DeleteOptions, ErrorCode, InodeId, LoonFs, Namespace, PinId, ReadAt,
    ReadOnly, RevisionNo, SharedObjectStore, StatOptions, Writable,
};
use loonfs_core::publish::NamespaceCommitEngine;
use loonfs_core::time::Deadline;
use loonfs_core::MutationContext;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_types::{
    AccessGrants, AccessRight, AttributeInclusion, ExpiredDeletion, NamespaceAccess, NamespaceId,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Commits the writer's expiry of one deletion through a commit engine of
/// its own.
async fn expire(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
) {
    let maintenance = loonfs_core::NamespaceWriterEngine::writer(
        store.clone(),
        namespace_id.clone(),
        writer_id("expiry-writer"),
        Arc::new(loonfs_core::test_support::content_writes()),
    );
    maintenance.fold_wal().await.expect("fold the deletion");
    maintenance
        .advance_retention_floor(loonfs_core::RetentionTarget::Seq(deletion_seq))
        .await
        .expect("advance the floor to the deletion");
    NamespaceCommitEngine::new(
        namespace_id.clone(),
        Arc::new(loonfs_core::test_support::content_writes()),
    )
    .publish_expiry(
        store,
        CommitId::generate(),
        vec![ExpiredDeletion {
            inode_id,
            deletion_seq,
        }],
        &MutationContext {
            writer_id: writer_id("expiry-writer"),
            now_ms: 1_000,
        },
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .results
    .pop()
    .expect("one result")
    .expect("expire the deletion");
}

/// A runtime that confirms the head on every read, so it sees a commit that
/// another engine made.
async fn reader(store: &SharedObjectStore) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(store.clone())
        .read_only()
        .max_staleness_ms(0)
        .build()
        .await
        .expect("reader")
}

fn code<T>(result: loonfs::Result<T>) -> Result<(), ErrorCode> {
    result.map(|_| ()).map_err(|error| error.code())
}

type Outcomes<const N: usize> = [(&'static str, Result<(), ErrorCode>); N];

/// The outcome of every read by inode ID: first the reads of the inode's
/// current entry and content, then the reads of its history.
async fn reads_by_inode(
    namespace: &Namespace<ReadOnly>,
    inode_id: InodeId,
) -> (Outcomes<5>, Outcomes<4>) {
    let without_attributes = StatOptions {
        include_attributes: AttributeInclusion::Omit,
        ..StatOptions::default()
    };
    let current = [
        (
            "stat",
            code(
                namespace
                    .stat_by_inode_with_options(inode_id, &without_attributes)
                    .await,
            ),
        ),
        ("attributes", code(namespace.stat_by_inode(inode_id).await)),
        (
            "content",
            code(namespace.read_file_by_inode(inode_id).await),
        ),
        (
            "content stream",
            code(namespace.read_file_stream_by_inode(inode_id).await),
        ),
        (
            "download",
            code(namespace.create_download_by_inode(inode_id).await),
        ),
    ];
    let revision = RevisionNo(1);
    let history = [
        (
            "revisions",
            code(
                namespace
                    .list_file_revisions_by_inode(inode_id)
                    .page()
                    .await,
            ),
        ),
        (
            "revision",
            code(
                namespace
                    .read_file_revision_by_inode(inode_id, revision)
                    .await,
            ),
        ),
        (
            "revision stream",
            code(
                namespace
                    .read_file_revision_stream_by_inode(inode_id, revision)
                    .await,
            ),
        ),
        (
            "revision download",
            code(
                namespace
                    .create_revision_download_by_inode(inode_id, revision)
                    .await,
            ),
        ),
    ];
    (current, history)
}

fn recursive() -> DeleteOptions {
    DeleteOptions {
        behavior: DeleteDirectoryBehavior::Recursive,
        ..DeleteOptions::default()
    }
}

#[tokio::test]
async fn every_read_by_inode_refuses_an_expired_subtree_and_serves_a_recoverable_deletion() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = namespace_id("demo");
    let actor = test_actor();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("expired-reads"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let mut inodes = BTreeMap::new();
    for path in ["/kept.txt", "/old/draft.txt", "/old/notes.txt"] {
        namespace.put_file(path, b"one", &actor).await.expect("put");
        inodes.insert(path, namespace.stat(path).await.expect("stat").inode_id);
    }
    let old = namespace
        .stat("/old")
        .await
        .expect("stat directory")
        .inode_id;
    // `/old/draft.txt` is deleted on its own first, so its deletion stays
    // recoverable after the deletion of `/old` around it expires.
    for path in ["/kept.txt", "/old/draft.txt"] {
        namespace.delete_path(path, &actor).await.expect("delete");
    }
    let deletion = namespace
        .delete_path_with_options("/old", &actor, &recursive())
        .await
        .expect("delete directory");
    expire(&store, &namespace_id, old, deletion.committed_seq).await;

    let namespace = reader(&store).await.namespace(&namespace_id);
    let not_found = Err(ErrorCode::InodeNotFound);
    for (path, recoverable) in [
        ("/kept.txt", true),
        ("/old/draft.txt", true),
        ("/old/notes.txt", false),
    ] {
        let (current, history) = reads_by_inode(&namespace, inodes[path]).await;
        // A deleted inode has no current entry or content, so only the
        // history reads serve a recoverable deletion.
        for (read, outcome) in current {
            assert_eq!(outcome, not_found, "{path}: {read}");
        }
        let expected = if recoverable { Ok(()) } else { not_found };
        for (read, outcome) in history {
            assert_eq!(outcome, expected, "{path}: {read}");
        }
    }
}

fn subject(subject_id: &str, principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(subject_id).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn access(boundary: bool, principal: &str, rights: &[AccessRight]) -> AccessState {
    AccessState {
        boundary,
        grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse(principal).expect("principal"),
            rights.iter().copied().collect(),
        )]))
        .expect("grants"),
    }
}

/// The history reads by inode ID that each subject can make, the last one
/// through the snapshot `snapshot_id`.
async fn history_reads(
    reader: &LoonFs<ReadOnly>,
    namespace_id: &NamespaceId,
    subjects: &[Subject],
    inode_id: InodeId,
    snapshot_id: &PinId,
) -> Vec<(String, [Result<(), ErrorCode>; 4])> {
    let at_snapshot = StatOptions {
        at: ReadAt::Snapshot(snapshot_id.clone()),
        ..StatOptions::default()
    };
    let mut outcomes = Vec::new();
    for subject in subjects {
        let namespace = reader.with_subject(subject.clone()).namespace(namespace_id);
        outcomes.push((
            subject.subject_id.to_string(),
            [
                code(
                    namespace
                        .list_file_revisions_by_inode(inode_id)
                        .page()
                        .await,
                ),
                code(
                    namespace
                        .read_file_revision_by_inode(inode_id, RevisionNo(1))
                        .await,
                ),
                code(
                    namespace
                        .create_revision_download_by_inode(inode_id, RevisionNo(1))
                        .await,
                ),
                code(
                    namespace
                        .stat_by_inode_with_options(inode_id, &at_snapshot)
                        .await,
                ),
            ],
        ));
    }
    outcomes
}

/// The example in the retention window design note: `/team` grants a team
/// `read` and `history`, and `/team/hr` sets its boundary and grants the same
/// rights to one person. A snapshot from before the deletion still serves
/// the administrator after the expiry.
#[tokio::test]
async fn once_a_deletion_expires_no_principal_reads_its_subtree_by_inode() {
    use AccessRight::{History, Read};

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = namespace_id("acl-expiry");
    let actor = test_actor();
    let admin = subject("root", "prn_root");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("acl-expiry"))
        .build()
        .await
        .expect("writer")
        .with_subject(admin.clone());
    writer
        .create_namespace_with_options(
            &namespace_id,
            &actor,
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: access(false, "prn_root", &[AccessRight::Admin]).grants,
                },
                ..Default::default()
            },
        )
        .await
        .expect("create namespace");
    let namespace: Namespace<Writable> = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for (path, state) in [
        ("/team", access(false, "team", &[Read, History])),
        ("/team/hr", access(true, "ada", &[Read, History])),
    ] {
        namespace
            .create_directory(path, &actor)
            .await
            .expect("create directory");
        namespace
            .update_access(path, &actor, state)
            .await
            .expect("update access");
    }
    namespace
        .put_file("/team/hr/salaries.csv", b"salaries", &actor)
        .await
        .expect("put file");
    let hr = namespace.stat("/team/hr").await.expect("stat").inode_id;
    let salaries = namespace
        .stat("/team/hr/salaries.csv")
        .await
        .expect("stat")
        .inode_id;
    let snapshot_id = namespace
        .create_snapshot("before", &CreateSnapshotOptions::default())
        .await
        .expect("create snapshot")
        .snapshot_id;
    let deletion = namespace
        .delete_path_with_options("/team/hr", &actor, &recursive())
        .await
        .expect("delete directory");

    let reader = reader(&store).await;
    let subjects = [subject("bob", "team"), subject("ada", "ada"), admin];
    let ok = [Ok(()); 4];
    let not_found = [Err(ErrorCode::InodeNotFound); 4];
    let reads = || history_reads(&reader, &namespace_id, &subjects, salaries, &snapshot_id);
    assert_eq!(
        reads().await,
        vec![
            ("bob".to_owned(), not_found),
            ("ada".to_owned(), ok),
            ("root".to_owned(), ok),
        ],
        "while the deletion is recoverable, the boundary keeps the team out"
    );
    expire(&store, &namespace_id, hr, deletion.committed_seq).await;
    let mut snapshot_only = not_found;
    snapshot_only[3] = Ok(());
    assert_eq!(
        reads().await,
        vec![
            ("bob".to_owned(), not_found),
            ("ada".to_owned(), not_found),
            ("root".to_owned(), snapshot_only),
        ],
        "an expired subtree is unknown to every principal, apart from an \
         administrator's snapshot read"
    );
}

fn deletion_seqs(page: loonfs_types::ListTrashResponse) -> Vec<ChangeSeq> {
    page.entries
        .into_iter()
        .map(|entry| entry.deletion_seq)
        .collect()
}

#[tokio::test]
async fn a_fork_lists_only_the_deletions_it_made_and_its_source_keeps_its_own() {
    let admin = subject("root", "prn_root");
    for access_mode in [
        NamespaceAccess::Unrestricted {},
        NamespaceAccess::Acl {
            principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
            root_grants: access(false, "prn_root", &[AccessRight::Admin]).grants,
        },
    ] {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let (source_id, fork_id) = (namespace_id("source"), namespace_id("fork"));
        let actor = test_actor();
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(writer_id("fork-trash"))
            .build()
            .await
            .expect("writer")
            .with_subject(admin.clone());
        writer
            .create_namespace_with_options(
                &source_id,
                &actor,
                &CreateNamespaceOptions {
                    access: access_mode.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("create namespace");
        let source = writer.open_namespace(&source_id).expect("open source");
        for path in ["/inherited.txt", "/forked.txt"] {
            source.put_file(path, b"bytes", &actor).await.expect("put");
        }
        let inherited = source
            .delete_path("/inherited.txt", &actor)
            .await
            .expect("delete in the source")
            .committed_seq;
        writer
            .fork_namespace(&source_id, &fork_id, &actor)
            .await
            .expect("fork");

        // The reader holds no writer session, and nothing has written to the
        // fork yet.
        let reader = reader(&store).await.with_subject(admin.clone());
        let trash = |namespace_id: &NamespaceId| {
            let namespace = reader.namespace(namespace_id);
            async move { deletion_seqs(namespace.list_trash().page().await.expect("list trash")) }
        };
        assert_eq!(
            trash(&fork_id).await,
            Vec::<ChangeSeq>::new(),
            "{access_mode:?}: a new fork"
        );
        let own = writer
            .open_namespace(&fork_id)
            .expect("open fork")
            .delete_path("/forked.txt", &actor)
            .await
            .expect("delete in the fork")
            .committed_seq;
        assert_eq!(trash(&fork_id).await, [own], "{access_mode:?}: the fork");
        assert_eq!(
            trash(&source_id).await,
            [inherited],
            "{access_mode:?}: the source"
        );
    }
}
