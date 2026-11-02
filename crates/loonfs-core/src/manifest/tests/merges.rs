//! The merge index: one row for each merge, written from the WAL tail and by
//! a fold, read newest first for each fork, and compacted with the commits
//! group.

use super::expired_deletion_rows::{compact_group, family_rows};
use super::*;
use loonfs_types::format::manifest::MergeIndexRecord;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::Commit;

fn fork(name: &str) -> NamespaceId {
    NamespaceId::parse(name).expect("namespace")
}

/// A commit that creates the directory `/{name}` under the commit ID `name`.
fn create_directory(name: &str) -> CommitIntent {
    CommitIntent::single(
        CommitId::parse(name).expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{name}")).expect("path"),
            parents: false,
        },
    )
}

async fn publish<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    candidate: CommitCandidate,
) -> Commit {
    crate::test_support::NamespaceFixture::new(store, namespace_id, &test_context())
        .publish_candidates(vec![candidate])
        .await
        .pop()
        .expect("one result")
        .expect("publish the commit")
}

/// Publishes a merge of `fork_name`'s commits above `after_seq`, through
/// `through_seq`, from a fork made at `fork_point_seq`.
async fn publish_merge<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    fork_name: &str,
    fork_point_seq: u64,
    after_seq: u64,
    through_seq: u64,
) -> Commit {
    let merge = MergeRecord {
        source_namespace_id: fork(fork_name),
        source_after_seq: ChangeSeq(after_seq),
        source_through_seq: ChangeSeq(through_seq),
    };
    let intent = create_directory(&format!("merge-{fork_name}-{through_seq}"));
    let fork_inode_id = InodeId::generate(ChangeSeq(through_seq), NonZeroU32::MIN);
    let candidate = CommitCandidate::new(intent).with_merge(
        merge,
        ChangeSeq(fork_point_seq),
        BTreeMap::from([(0, fork_inode_id)]),
    );
    publish(store, namespace_id, candidate).await
}

/// The merge index row of a published merge.
fn indexed(commit: &Commit) -> MergeIndexRecord {
    let merge = commit.merge.as_ref().expect("a merge");
    MergeIndexRecord {
        source_namespace_id: merge.source_namespace_id.clone(),
        committed_seq: commit.committed_seq,
        source_after_seq: merge.source_after_seq,
        source_through_seq: merge.source_through_seq,
    }
}

async fn newest_merge<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    fork_name: &str,
) -> Option<MergeIndexRecord> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .newest_merge(&fork(fork_name))
        .await
        .expect("merge lookup")
}

/// Checks that the projected tail and a fold both produce a merge index row
/// for each merge, and that a scan of one fork's prefix returns that fork's
/// merges newest first, and no other fork's.
#[tokio::test]
async fn merges_are_indexed_in_the_tail_and_the_fold_newest_first_for_each_fork() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let first = publish_merge(&store, &namespace_id, "fork-a", 3, 3, 7).await;
    let other = publish_merge(&store, &namespace_id, "fork-b", 0, 0, 4).await;
    let repeat = publish_merge(&store, &namespace_id, "fork-a", 3, 7, 9).await;
    let expected = [indexed(&repeat), indexed(&first), indexed(&other)].map(MetadataRow::Merge);

    let tail_rows = crate::test_support::namespace_store(&store, &namespace_id)
        .load_manifest_projection(crate::test_support::observed_now())
        .await
        .expect("projection")
        .tail_state
        .merged_rows();
    assert_eq!(
        manifest_rows_for_family(&tail_rows, ApiMetadataRowFamily::Merges),
        expected
    );
    assert_eq!(
        newest_merge(&store, &namespace_id, "fork-a").await,
        Some(indexed(&repeat))
    );

    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the merges");
    assert_eq!(
        family_rows(&store, &namespace_id, ApiMetadataRowFamily::Merges).await,
        expected
    );
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let fork_a = super::super::metadata_basis_from_manifest(&store, None, &current)
        .segments
        .scan_prefix(
            ApiMetadataRowFamily::Merges,
            &lookup_keys::merge_prefix(&fork("fork-a")),
        )
        .await
        .expect("scan the fork's merges");
    assert_eq!(fork_a, expected[..2]);
    for (fork_name, newest) in [
        ("fork-a", Some(indexed(&repeat))),
        ("fork-b", Some(indexed(&other))),
        ("fork", None),
    ] {
        assert_eq!(
            newest_merge(&store, &namespace_id, fork_name).await,
            newest,
            "{fork_name}"
        );
    }
}

/// Checks that a base compaction of the commits group removes the merge
/// index row of a merge below the retention floor, together with its commit
/// row and receipt, and keeps the rows at and above the floor.
#[tokio::test]
async fn merge_rows_compact_with_the_commits_group_and_leave_no_row_below_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let first = publish_merge(&store, &namespace_id, "fork-a", 3, 3, 7).await;
    let floor = publish(
        &store,
        &namespace_id,
        CommitCandidate::new(create_directory("between")),
    )
    .await
    .committed_seq;
    let repeat = publish_merge(&store, &namespace_id, "fork-a", 3, 7, 9).await;
    assert!(first.committed_seq < floor && floor < repeat.committed_seq);
    advance_floor_through(&store, &namespace_id, floor, &context).await;
    compact_group(
        &store,
        &namespace_id,
        &context,
        MetadataFamilyGroup::Commits,
    )
    .await;

    assert_eq!(
        family_rows(&store, &namespace_id, ApiMetadataRowFamily::Merges).await,
        [MetadataRow::Merge(indexed(&repeat))]
    );
    for family in [
        ApiMetadataRowFamily::Commits,
        ApiMetadataRowFamily::CommitReceipts,
    ] {
        let kept: BTreeSet<_> = family_rows(&store, &namespace_id, family)
            .await
            .iter()
            .map(row::manifest_row_commit_seq)
            .collect();
        assert_eq!(
            kept,
            BTreeSet::from([floor, repeat.committed_seq]),
            "{family:?}"
        );
    }
    assert_eq!(
        newest_merge(&store, &namespace_id, "fork-a").await,
        Some(indexed(&repeat))
    );
}
