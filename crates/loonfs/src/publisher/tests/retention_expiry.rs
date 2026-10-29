//! A writer session that undeletes a deletion after another process's
//! retention advance passed it, while the session still plans against the
//! manifest before the advance.

use super::*;
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::{DeleteDirectoryBehavior, InodeId};

/// The inode rows that the current manifest's segments store.
async fn inode_rows(store: &dyn ObjectStore, namespace_id: &NamespaceId) -> u64 {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_statistics()
        .await
        .expect("load the namespace's statistics")
        .inode_record_count
}

/// Runs compaction units through `maintenance` until none is needed, and
/// returns how many published.
async fn drain_compaction(maintenance: &crate::Maintenance, namespace_id: &NamespaceId) -> usize {
    for published in 0..32 {
        let response = maintenance
            .compact_metadata(namespace_id, &crate::MaintenanceCancellation::new())
            .await
            .expect("compact");
        if response.compaction == crate::MetadataCompactionOutcome::NotNeeded {
            return published;
        }
    }
    panic!("compaction kept publishing");
}

/// Creates `/deleted` with `/deleted/inner` in it through `namespace`'s
/// session.
async fn create_directories(namespace: &crate::Namespace<crate::Writable>) {
    for (commit_id, name) in [
        ("create-deleted", "deleted"),
        ("create-inner", "deleted/inner"),
    ] {
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                commit_id, name,
            )))
            .await
            .expect("create a directory");
    }
}

/// Deletes `/deleted` through `namespace`'s session, and returns the deleted
/// root with the deletion's sequence.
async fn delete_a_directory(namespace: &crate::Namespace<crate::Writable>) -> (InodeId, ChangeSeq) {
    let deleted = namespace
        .commit_candidate(CommitCandidate::new(CommitIntent::single(
            CommitId::parse("delete").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/deleted").expect("path"),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
        )))
        .await
        .expect("delete");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("the delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// Checks the race that the acknowledged floor closes. Another process folds
/// a deletion and advances the floor past it while the writer's session
/// still plans against the manifest before the advance, which it keeps until
/// its next successor check, so the session accepts an undelete of the
/// deletion. A base compaction over the advance's manifest keeps the
/// deletion's rows, because the acknowledged floor is still below the
/// deletion. Once the session's next commit, which records the new floor,
/// is folded, the deletion's latest event is the revoke, so no compaction
/// removes the restored item's rows.
#[tokio::test]
async fn an_undelete_against_an_older_floor_keeps_its_rows_through_every_compaction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    // The session trusts its basis without a successor check until its
    // monotonic clock moves.
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .monotonic_timer(clock.clone())
        .build()
        .await
        .expect("build writer");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let maintainer_clock = Arc::new(ManualClock::at_system_time());
    maintainer_clock.advance_ms(crate::GC_DEFAULT_GRACE_WINDOW_MS);
    let maintainer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintainer"))
        .wall_clock(maintainer_clock)
        .build()
        .await
        .expect("build the maintainer");
    let maintenance = maintainer.maintenance(loonfs_test_support::ids::writer_id("maintainer"));
    // Two folds leave a delta run above the first, so a compaction has a
    // window to merge.
    create_directories(&namespace).await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the directories");
    let (inode_id, deletion_seq) = delete_a_directory(&namespace).await;
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the deletion");
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, deletion_seq);

    namespace
        .undelete(inode_id, deletion_seq, &actor)
        .await
        .expect("an undelete against the old floor");
    let rows = inode_rows(store.as_ref(), &namespace_id).await;
    assert!(drain_compaction(&maintenance, &namespace_id).await > 0);
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.retention_floor_seq, deletion_seq);
    assert_eq!(diagnostics.acknowledged_floor_seq, ChangeSeq(0));
    assert_eq!(
        inode_rows(store.as_ref(), &namespace_id).await,
        rows,
        "a compaction over the advance's manifest keeps the deletion's rows"
    );

    // Once the session's basis is older than the publication budget, its
    // next commit checks for a successor, plans against the advance's floor,
    // and records that floor.
    clock.advance_ms(loonfs_core::limits::WAL_PUBLISH_BUDGET_MS + 1);
    namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "later", "later",
        )))
        .await
        .expect("a commit after the undelete");
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the undelete and the later commit");
    let diagnostics = maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics.acknowledged_floor_seq, deletion_seq);
    let rows = inode_rows(store.as_ref(), &namespace_id).await;
    assert!(drain_compaction(&maintenance, &namespace_id).await > 0);
    assert_eq!(
        inode_rows(store.as_ref(), &namespace_id).await,
        rows,
        "no compaction removes the restored item's rows"
    );
    let reader = maintainer.read_only().namespace(&namespace_id);
    for path in ["/deleted", "/deleted/inner"] {
        reader
            .stat(path)
            .await
            .unwrap_or_else(|error| panic!("{path} is restored: {error}"));
    }
}
