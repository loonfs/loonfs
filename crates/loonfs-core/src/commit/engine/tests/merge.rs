//! Merge commits: the merge record in the WAL, the change feed, and the
//! commit rows, a merge's retry identity, the range rule that makes each
//! merge continue the previous merge from its fork, and the refusal of a merge
//! whose fork point is below the retention floor.

mod rebase;

use super::*;
use crate::changes::list_changes_after;
use crate::compaction::{CompactionUnitOutcome, MetadataCompactionCancellation};
use crate::manifest::RetentionTarget;
use crate::test_support::NamespaceFixture;
use loonfs_types::format::wal::{ForkRange, MergeRecord, MAX_WAL_OBJECT_BYTES};
use loonfs_types::{AbsolutePath, CompactorEpoch, DisplayName, EffectiveLimit, InodeId, Merge};
use std::collections::BTreeMap;

/// The sequence of `main` that the tests' fork was made at.
const FORK_POINT: CommitSeq = CommitSeq(3);

fn fork() -> NamespaceId {
    NamespaceId::parse("main-fork").expect("namespace")
}

/// A merge of the fork's commits above `after_seq`, through `through_seq`.
fn merge(after_seq: u64, through_seq: u64) -> MergeRecord {
    MergeRecord {
        source_namespace_id: fork(),
        source_after_seq: CommitSeq(after_seq),
        source_through_seq: CommitSeq(through_seq),
        covered_ranges: Vec::new(),
        conflicts: Vec::new(),
    }
}

fn reported(merge: &MergeRecord) -> Merge {
    Merge {
        source_namespace_id: merge.source_namespace_id.clone(),
        source_after_seq: merge.source_after_seq,
        source_through_seq: merge.source_through_seq,
    }
}

/// An ID that an inode created in the fork has.
fn fork_inode_id() -> InodeId {
    InodeId::generate(CommitSeq(FORK_POINT.0 + 1), std::num::NonZeroU32::MIN)
}

/// A merge of `merge` under `commit_id` that creates `/{name}` under an inode
/// ID from the fork.
fn merge_candidate(commit_id: &str, name: &str, merge: MergeRecord) -> CommitCandidate {
    CommitCandidate::new(create_directory_request(commit_id, name)).with_merge(
        merge,
        FORK_POINT,
        BTreeMap::from([(0, fork_inode_id())]),
    )
}

async fn setup() -> (tempfile::TempDir, LocalFsStore, NamespaceId, WriterAt) {
    crate::test_support::created_namespace("main", context("writer")).await
}

async fn publish(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    writer: &WriterAt,
    candidate: CommitCandidate,
) -> Result<Commit> {
    NamespaceFixture::new(store, namespace_id, writer)
        .publish_candidates(vec![candidate])
        .await
        .pop()
        .expect("one result")
}

async fn feed(store: &LocalFsStore, namespace_id: &NamespaceId) -> Vec<Commit> {
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("read view");
    list_changes_after(
        &view,
        loonfs_types::ChangeFeedPosition::after(CommitSeq(0)),
        EffectiveLimit::MAX,
        loonfs_types::options::CommitSplitting::Whole,
    )
    .await
    .expect("change feed")
    .changes
}

/// Checks that the merge record reaches the commit response, the change feed
/// while the commit is in the WAL tail, and the change feed after a fold
/// writes the commit row, and that an ordinary commit reports none.
#[tokio::test]
async fn the_feed_reports_a_merge_record_before_and_after_a_fold() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let ordinary = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(create_directory_request("ordinary", "ordinary")),
    )
    .await
    .expect("ordinary commit");
    assert_eq!(ordinary.merge, None);
    let merged = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("merge", "merged", merge(3, 7)),
    )
    .await
    .expect("merge");
    assert_eq!(merged.merge, Some(reported(&merge(3, 7))));

    for folded in [false, true] {
        if folded {
            crate::test_support::namespace_store(&store, &namespace_id)
                .fold_wal()
                .await
                .expect("fold");
        }
        let changes = feed(&store, &namespace_id).await;
        assert_eq!(
            changes,
            [ordinary.clone(), merged.clone()],
            "folded: {folded}"
        );
    }
}

/// Checks that a merge's identity is its merge record: a retry with the same
/// commit ID and merge record replays the original commit whatever its
/// operations, before and after a fold, and a different range conflicts.
#[tokio::test]
async fn a_reused_commit_id_replays_the_same_merge_and_conflicts_with_another_range() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let original = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("merge", "merged", merge(3, 7)),
    )
    .await
    .expect("merge");

    for folded in [false, true] {
        if folded {
            crate::test_support::namespace_store(&store, &namespace_id)
                .fold_wal()
                .await
                .expect("fold");
        }
        let replayed = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("merge", "planned-again", merge(3, 7)),
        )
        .await
        .expect("replay");
        assert_eq!(replayed, original, "folded: {folded}");

        let error = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("merge", "merged", merge(3, 8)),
        )
        .await
        .expect_err("a different range reuses the commit ID");
        assert_matches!(
            error,
            CoreError::CommitIdReuseConflict { committed: Some(receipt), .. }
                if receipt.committed_seq == original.committed_seq,
            "folded: {folded}"
        );
    }
    assert_eq!(feed(&store, &namespace_id).await, [original]);
}

/// Checks a refused merge's error, its wire code, and its details.
fn assert_range_conflict(error: CoreError, expected_after_seq: u64, actual_after_seq: u64) {
    assert_eq!(error.code(), ErrorCode::MergeRangeConflict);
    assert_eq!(error.code().kind(), loonfs_types::ErrorKind::Conflict);
    let details = error.details().expect("details");
    assert_eq!(details.source_namespace_id, Some(fork()));
    assert_eq!(
        details.expected_after_seq,
        Some(CommitSeq(expected_after_seq))
    );
    assert_eq!(details.actual_after_seq, Some(CommitSeq(actual_after_seq)));
    assert_matches!(
        error,
        CoreError::MergeRangeConflict { source_namespace_id, .. } if source_namespace_id == fork()
    );
}

/// Checks that a fork's first merge has to start at the fork point.
#[tokio::test]
async fn a_first_merge_starts_at_the_fork_point() {
    let (_directory, store, namespace_id, writer) = setup().await;
    for after_seq in [2, 4] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("early-or-late", "skipped", merge(after_seq, 7)),
        )
        .await
        .expect_err("a first merge that doesn't start at the fork point");
        assert_range_conflict(error, FORK_POINT.0, after_seq);
    }
    let merged = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("first", "first", merge(FORK_POINT.0, 7)),
    )
    .await
    .expect("a first merge at the fork point");
    assert_eq!(merged.merge, Some(reported(&merge(FORK_POINT.0, 7))));
}

/// Checks that a repeat merge has to start where the previous merge stopped,
/// whether the previous merge's index row is in a folded run or in the WAL
/// tail, and that a retry of the previous merge with its original commit ID
/// still replays, because the receipt check runs before the range rule.
#[tokio::test]
async fn a_repeat_merge_continues_the_previous_merge_exactly() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let first = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("first", "first", merge(3, 7)),
    )
    .await
    .expect("first merge");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the first merge");

    for (after_seq, why) in [(5, "an overlap"), (8, "a gap")] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("repeat", "repeat", merge(after_seq, 9)),
        )
        .await
        .expect_err(why);
        assert_range_conflict(error, 7, after_seq);
    }
    publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("repeat", "repeat", merge(7, 9)),
    )
    .await
    .expect("a merge that continues the first");
    let error = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("third", "third", merge(7, 12)),
    )
    .await
    .expect_err("a merge that repeats the merge in the WAL tail");
    assert_range_conflict(error, 9, 7);

    let replayed = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("first", "planned-again", merge(3, 7)),
    )
    .await
    .expect("a retry of the first merge");
    assert_eq!(replayed, first);
}

/// Checks that each range of another fork that a merge record covers has to
/// continue that fork's newest row in the merge index, whether a covered
/// range or the fork's own merge wrote the row, and that a covered range of
/// a fork with no row is admitted where it starts, because nothing in the
/// namespace contradicts it.
#[tokio::test]
async fn a_covered_range_continues_its_forks_newest_row() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let original = NamespaceId::parse("main-original").expect("namespace");
    let covering = |after_seq, through_seq, covered_after_seq, covered_through_seq| MergeRecord {
        covered_ranges: vec![ForkRange {
            source_namespace_id: original.clone(),
            source_after_seq: CommitSeq(covered_after_seq),
            source_through_seq: CommitSeq(covered_through_seq),
        }],
        ..merge(after_seq, through_seq)
    };
    publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("first", "first", covering(3, 7, 11, 20)),
    )
    .await
    .expect("a covered range of a fork with no row");
    for (covered_after_seq, why) in [(15, "an overlap"), (21, "a gap")] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("second", "second", covering(7, 9, covered_after_seq, 25)),
        )
        .await
        .expect_err(why);
        assert_eq!(error.code(), ErrorCode::MergeRangeConflict, "{why}");
        assert_matches!(
            error,
            CoreError::MergeRangeConflict {
                source_namespace_id,
                expected_after_seq: CommitSeq(20),
                actual_after_seq,
            } if source_namespace_id == original && actual_after_seq == CommitSeq(covered_after_seq),
            "{why}"
        );
    }
    let own = MergeRecord {
        source_namespace_id: original.clone(),
        ..merge(20, 22)
    };
    publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("original", "original", own),
    )
    .await
    .expect("the fork's own merge continues the covered range");
    let error = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("stale", "stale", covering(7, 9, 20, 25)),
    )
    .await
    .expect_err("a covered range that the fork's own merge applied");
    assert_matches!(
        error,
        CoreError::MergeRangeConflict {
            expected_after_seq: CommitSeq(22),
            actual_after_seq: CommitSeq(20),
            ..
        }
    );
    publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("second", "second", covering(7, 9, 22, 25)),
    )
    .await
    .expect("a covered range that continues the fork's own merge");
}

fn intent(commit_id: &str, operations: Vec<FilesystemOperation>) -> CommitIntent {
    CommitIntent {
        commit_id: CommitId::parse(commit_id).expect("commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations,
        preconditions: Vec::new(),
    }
}

/// Checks that a merge with no operations commits its record, which the
/// merge index and the change feed report, while an ordinary commit with no
/// operations is still refused.
#[tokio::test]
async fn a_merge_with_no_operations_commits_its_record() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let error = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(intent("empty", Vec::new())),
    )
    .await
    .expect_err("an ordinary commit with no operations");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let record = merge(3, 7);
    let merged = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(intent("empty-merge", Vec::new())).with_merge(
            record.clone(),
            FORK_POINT,
            BTreeMap::new(),
        ),
    )
    .await
    .expect("a merge with no operations");
    assert!(merged.events.is_empty());
    assert_eq!(merged.merge, Some(reported(&record)));
    assert_eq!(feed(&store, &namespace_id).await, [merged]);
}

/// Checks that a merge isn't held to the operation count that limits a
/// request body, and that its creates make inodes under the fork's IDs.
#[tokio::test]
async fn a_merge_of_ten_thousand_creates_is_admitted_where_an_explicit_commit_is_refused() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let creates = intent(
        "many",
        (0..10_000)
            .map(|index| FilesystemOperation::CreateDirectoryByInode {
                parent_inode_id: loonfs_types::ROOT_INODE_ID,
                display_name: loonfs_types::DisplayName::parse(format!("directory-{index}"))
                    .expect("display name"),
            })
            .collect(),
    );
    let error = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(creates.clone()),
    )
    .await
    .expect_err("an explicit commit over the operation limit");
    assert_matches!(
        error,
        CoreError::InvalidCommitRequest(message) if message.contains("10000 operations")
    );

    let fork_ids: BTreeMap<usize, InodeId> =
        (0..10_000).map(|index| (index, fork_inode_id())).collect();
    let merged = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(creates).with_merge(merge(3, 7), FORK_POINT, fork_ids.clone()),
    )
    .await
    .expect("a merge of the same creates");
    let created: Vec<InodeId> = merged
        .events
        .iter()
        .map(|event| match event {
            loonfs_types::FilesystemEvent::DirectoryCreated { inode_id, .. } => *inode_id,
            other => panic!("expected a directory creation, got {other:?}"),
        })
        .collect();
    assert_eq!(created, fork_ids.into_values().collect::<Vec<_>>());
}

/// Checks that a merge whose planned record doesn't fit in one WAL object
/// fails with `merge_too_large`, which has the 413 kind, and writes no WAL
/// object.
#[tokio::test]
async fn a_merge_over_the_wal_object_bound_fails_with_merge_too_large_and_writes_nothing() {
    let (_directory, store, namespace_id, writer) = setup().await;
    // One engine publishes both commits, so the merge's publication acquires
    // no writer epoch and writes no fence.
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    let directory = engine
        .publish_batch(vec![create_dir("directory", "directory")], &writer.clock())
        .await
        .results
        .pop()
        .expect("one result")
        .expect("create a directory");
    let loonfs_types::FilesystemEvent::DirectoryCreated { inode_id, .. } = directory.events[0]
    else {
        panic!("expected a directory creation");
    };
    let updates = |count: usize| {
        let operations = (0..count)
            .map(|index| FilesystemOperation::UpdateAttributesByInode {
                inode_id,
                set: BTreeMap::from([(
                    loonfs_types::AttributeKey::parse("index").expect("key"),
                    loonfs_types::AttributeValue::parse(index.to_string()).expect("value"),
                )]),
                remove: Vec::new(),
                expected_attributes_revision_no: None,
            })
            .collect();
        CommitCandidate::new(intent("too-large", operations)).with_merge(
            merge(3, 7),
            FORK_POINT,
            BTreeMap::new(),
        )
    };
    let per_update =
        updates(2).wal_record_bytes_upper_bound() - updates(1).wal_record_bytes_upper_bound();
    let count = MAX_WAL_OBJECT_BYTES / per_update + 1;
    let before = wal_object_count(&store, &namespace_id).await;

    let error = engine
        .publish_batch(vec![updates(count)], &writer.clock())
        .await
        .results
        .pop()
        .expect("one result")
        .expect_err("a merge over the bound");
    assert_matches!(
        error,
        CoreError::MergeTooLarge {
            bound: crate::error::MergeBound::WalObject,
            estimated_bytes,
            max_bytes,
            split_at_seq: None,
        } if estimated_bytes > max_bytes && max_bytes == MAX_WAL_OBJECT_BYTES
    );
    assert_eq!(error.code(), ErrorCode::MergeTooLarge);
    assert_eq!(
        error.code().kind(),
        loonfs_types::ErrorKind::ContentTooLarge
    );
    assert_eq!(wal_object_count(&store, &namespace_id).await, before);
}

/// Checks that a merge refers only to content that the fork already wrote:
/// inline content and appended bytes are refused.
#[tokio::test]
async fn a_merge_with_inline_content_or_an_append_is_refused() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let inline = InlineContent::new(
        ContentId::generate(),
        bytes::Bytes::from_static(b"inline"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let put = intent(
        "inline",
        vec![FilesystemOperation::CreateFileByInode {
            parent_inode_id: loonfs_types::ROOT_INODE_ID,
            display_name: loonfs_types::DisplayName::parse("inline.txt").expect("name"),
            content: loonfs_types::FileContent::Ref(inline.content_ref().clone()),
        }],
    );
    let append = intent(
        "append",
        vec![FilesystemOperation::AppendFile {
            path: loonfs_types::AbsolutePath::parse("/log").expect("path"),
            inline_content: b"appended".to_vec(),
            expected_inode_id: None,
            expected_revision_no: None,
        }],
    );
    for candidate in [
        CommitCandidate::with_inline_content(put, Vec::new(), vec![inline]).with_merge(
            merge(3, 7),
            FORK_POINT,
            BTreeMap::from([(0, fork_inode_id())]),
        ),
        CommitCandidate::new(append).with_merge(merge(3, 7), FORK_POINT, BTreeMap::new()),
    ] {
        let error = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect_err("a merge with bytes of its own");
        assert_matches!(
            error,
            CoreError::InvalidCommitRequest(message)
                if message == "a merge includes no inline content or appended bytes"
        );
    }
}

/// A file that a fork created, merged into the namespace it was forked
/// from.
struct MergedFile {
    store: RecordingStore<LocalFsStore>,
    main: NamespaceId,
    fork: NamespaceId,
    inode_id: InodeId,
    bytes: Vec<u8>,
}

/// Forks `main`, writes a 1 MiB file in the fork, folds the fork, and merges
/// the file into `main` the way the merge planner does: a create under the
/// fork's inode ID, with the reference and layout that the fork's rows
/// record. The store records content requests, and its counters are reset
/// just before the merge.
async fn merge_a_forks_file() -> (tempfile::TempDir, MergedFile) {
    let directory = tempdir().expect("tempdir");
    let [main, fork] = ["main", "main-fork"].map(|name| NamespaceId::parse(name).expect("ns"));
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    );
    let writer = context("writer");
    let main_fixture = NamespaceFixture::new(&store, &main, &writer);
    main_fixture.create().await.expect("create main");
    main_fixture
        .engine()
        .fork_namespace(
            &crate::test_support::administrator(&main_fixture.engine()),
            main_fixture
                .engine()
                .find_fork_target(&fork)
                .await
                .expect("find the fork target"),
            &loonfs_test_support::test_actor(),
            crate::test_support::fold_before_fork(&main_fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let fork_point_seq = crate::test_support::namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect("the fork's manifest")
        .payload()
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .manifest
        .head_seq;
    let bytes = vec![7_u8; 1 << 20];
    let written = NamespaceFixture::new(&store, &fork, &writer)
        .put_file(
            "/big.bin",
            &bytes,
            loonfs_types::DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("write the fork's file");
    let loonfs_types::FilesystemEvent::FileCreated {
        inode_id,
        content_ref,
        ..
    } = written.events[0].clone()
    else {
        panic!("expected a file creation");
    };
    crate::test_support::namespace_store(&store, &fork)
        .fold_wal()
        .await
        .expect("fold the fork");
    let layout = crate::test_support::namespace_store(&store, &fork)
        .load_current_metadata_view()
        .await
        .expect("the fork's view")
        .metadata_view()
        .content_layout(&content_ref.content_id)
        .await
        .expect("layout lookup")
        .expect("the fork's layout row")
        .layout;

    let create = intent(
        "merge-big",
        vec![FilesystemOperation::CreateFileByInode {
            parent_inode_id: loonfs_types::ROOT_INODE_ID,
            display_name: loonfs_types::DisplayName::parse("big.bin").expect("name"),
            content: loonfs_types::FileContent::Ref(content_ref.clone()),
        }],
    );
    let proof = PreparedContent::for_pool_member(main.clone(), content_ref, layout, u64::MAX);
    let candidate = CommitCandidate::prepared(create, vec![proof]).with_merge(
        MergeRecord {
            source_namespace_id: fork.clone(),
            source_after_seq: fork_point_seq,
            source_through_seq: written.committed_seq,
            covered_ranges: Vec::new(),
            conflicts: Vec::new(),
        },
        fork_point_seq,
        BTreeMap::from([(0, inode_id)]),
    );
    store.reset();
    main_fixture
        .publish_candidates(vec![candidate])
        .await
        .pop()
        .expect("one result")
        .expect("merge the file");
    let merged = MergedFile {
        store,
        main,
        fork,
        inode_id,
        bytes,
    };
    (directory, merged)
}

async fn stat<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> loonfs_types::PathEntry {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("view")
        .stat_inode(
            inode_id,
            loonfs_types::options::AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Service),
        )
        .await
        .expect("stat by inode ID")
}

/// Checks that a merge admits a fork's file from the reference and layout
/// that the fork's rows record, without reading the file's bytes. The test
/// store caches no content, so a read of the bytes would reach the store, as
/// the read after the merge does.
#[tokio::test]
async fn a_merge_of_a_forks_file_reads_none_of_its_bytes() {
    let (_directory, merged) = merge_a_forks_file().await;
    assert_eq!(merged.store.count(OperationSelector::Any), 0);

    let read = crate::test_support::namespace_store(&merged.store, &merged.main)
        .load_current_metadata_view()
        .await
        .expect("view")
        .read_file_bytes_by_inode(
            merged.inode_id,
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Service),
        )
        .await
        .expect("read the merged file");
    assert_eq!(read.bytes, merged.bytes);
    assert!(merged.store.count(OperationSelector::Read) > 0);
}

/// Checks that a merge creates a fork's inode under the fork's ID, so a stat
/// by that ID finds the same file in both namespaces.
#[tokio::test]
async fn a_merge_creates_the_forks_inode_under_the_forks_id() {
    let (_directory, merged) = merge_a_forks_file().await;
    let in_main = stat(&merged.store, &merged.main, merged.inode_id).await;
    let in_fork = stat(&merged.store, &merged.fork, merged.inode_id).await;
    assert_eq!(
        (&in_main.inode_id, &in_main.path),
        (&in_fork.inode_id, &in_fork.path)
    );
    let file = |entry: &loonfs_types::PathEntry| match &entry.kind {
        loonfs_types::PathEntryKind::File {
            revision_no,
            size_bytes,
            content_ref,
            ..
        } => (*revision_no, *size_bytes, content_ref.clone()),
        other => panic!("expected a file, got {other:?}"),
    };
    assert_eq!(file(&in_main), file(&in_fork));
}

/// Folds the WAL, advances the retention floor to `target`, compacts every
/// family group, and returns the new floor.
async fn advance_floor_and_compact(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    target: RetentionTarget,
) -> CommitSeq {
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let mut maintainer = NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "maintainer",
            crate::test_support::grace_elapsed_after(1_000),
        ),
    );
    maintainer.namespace.run_policy = crate::test_support::one_delta_run_policy();
    let advanced = maintainer
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor");
    let mut compacted = false;
    for _ in 0..32 {
        let outcome = maintainer
            .engine()
            .compact_metadata_unit(
                None,
                CompactorEpoch(0),
                &MetadataCompactionCancellation::default(),
            )
            .await
            .expect("compact a family group");
        if matches!(outcome, CompactionUnitOutcome::NotNeeded) {
            compacted = true;
            break;
        }
    }
    assert!(compacted, "a small namespace should finish compacting");
    advanced.response.retention_floor_seq
}

/// Checks that a merge is admitted while the retention floor is at its fork
/// point, and refused with `below_retention_floor` once the floor is above
/// the fork point. By then a compaction has removed the fork's merge index
/// rows, so without the refusal the writer would expect the next merge to
/// start at the fork point, and it would admit a merge that applies the
/// fork's merged commits again.
#[tokio::test]
async fn a_merge_whose_fork_point_is_below_the_retention_floor_is_refused() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let mut head = CommitSeq(0);
    while head < FORK_POINT {
        let name = format!("before-{}", head.0);
        let candidate = CommitCandidate::new(create_directory_request(&name, &name));
        head = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect("an ordinary commit")
            .committed_seq;
    }
    let first = merge_candidate("first", "first", merge(3, 7));
    publish(&store, &namespace_id, &writer, first)
        .await
        .expect("the first merge");
    let floor =
        advance_floor_and_compact(&store, &namespace_id, RetentionTarget::Seq(FORK_POINT)).await;
    assert_eq!(floor, FORK_POINT);
    let repeat = merge_candidate("repeat", "repeat", merge(7, 9));
    publish(&store, &namespace_id, &writer, repeat)
        .await
        .expect("a merge whose fork point is at the floor");

    let after = CommitCandidate::new(create_directory_request("after", "after"));
    publish(&store, &namespace_id, &writer, after)
        .await
        .expect("an ordinary commit after the merges");
    let floor = advance_floor_and_compact(&store, &namespace_id, RetentionTarget::Head).await;
    let index = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .newest_merge(&fork())
        .await
        .expect("merge lookup");
    assert_eq!(index, None, "the compaction removes the fork's merge rows");

    for after_seq in [FORK_POINT.0, 9] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            merge_candidate("third", "third", merge(after_seq, 12)),
        )
        .await
        .expect_err("a merge whose fork point is below the floor");
        assert_eq!(error.code(), ErrorCode::BelowRetentionFloor);
        let details = error.details().expect("details");
        assert_eq!(details.fork_point_seq, Some(FORK_POINT));
        assert_eq!(details.retention_floor_seq, Some(floor));
        assert_matches!(
            error,
            CoreError::BelowRetentionFloor {
                below: crate::error::HistoryPosition::ForkPoint { fork_point_seq },
                retention_floor_seq,
            } if fork_point_seq == FORK_POINT && retention_floor_seq == floor
        );
    }
}

/// The inode that a commit's first event created.
fn created_inode(commit: &Commit) -> InodeId {
    match &commit.events[0] {
        loonfs_types::FilesystemEvent::DirectoryCreated { inode_id, .. } => *inode_id,
        other => panic!("expected a directory creation, got {other:?}"),
    }
}

/// Checks that a merge that creates an inode under an ID that the namespace
/// already has, whether an ordinary commit or an earlier merge created it, or
/// under one ID twice, fails with `server_error` because its preparation
/// panics, since the range rule leaves only a planner bug to send one, and
/// that a refused merge publishes nothing.
#[tokio::test]
async fn a_merge_of_an_inode_id_that_already_exists_panics_in_its_preparation() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let ordinary = publish(
        &store,
        &namespace_id,
        &writer,
        create_dir("ordinary", "ordinary"),
    )
    .await
    .expect("an ordinary commit");
    let merged = publish(
        &store,
        &namespace_id,
        &writer,
        merge_candidate("first", "first", merge(3, 7)),
    )
    .await
    .expect("the first merge");
    let repeated = fork_inode_id();
    let creates = |names: &[&str]| {
        names
            .iter()
            .map(|name| FilesystemOperation::CreateDirectoryByInode {
                parent_inode_id: loonfs_types::ROOT_INODE_ID,
                display_name: loonfs_types::DisplayName::parse(*name).expect("display name"),
            })
            .collect::<Vec<_>>()
    };
    let cases = [
        (vec![(0, created_inode(&ordinary))], 0),
        (vec![(0, created_inode(&merged))], 0),
        (vec![(0, repeated), (1, repeated)], 1),
    ];
    for (assigned, refused_index) in cases {
        let inode_id = assigned[refused_index].1;
        let candidate = CommitCandidate::new(intent("again", creates(&["one", "two"]))).with_merge(
            merge(7, 9),
            FORK_POINT,
            assigned.into_iter().collect(),
        );
        let error = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect_err("a merge of an inode ID that exists");
        assert_eq!(error.code(), ErrorCode::ServerError);
        let message =
            format!("a merge should create inode `{inode_id}` only where it doesn't exist");
        assert_matches!(
            &error,
            CoreError::CommitPreparationPanicked(panic) if panic.contains(&message)
        );
    }
    assert_eq!(feed(&store, &namespace_id).await, [ordinary, merged]);
}

/// Checks that a merge whose inode IDs don't match its creates fails with
/// `server_error` when its preparation panics, for a create without an ID
/// and for an ID assigned to an operation that creates no inode. The error
/// isn't one after which a merge plans again, the same merge fails the
/// same way when it's sent again, and the namespace keeps committing.
#[tokio::test]
async fn a_merge_whose_inode_ids_dont_match_its_creates_is_refused() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let directory = publish(
        &store,
        &namespace_id,
        &writer,
        create_dir("directory", "directory"),
    )
    .await
    .expect("an ordinary commit");
    let update = FilesystemOperation::UpdateAttributesByInode {
        inode_id: created_inode(&directory),
        set: BTreeMap::from([(
            loonfs_types::AttributeKey::parse("key").expect("key"),
            loonfs_types::AttributeValue::parse("value").expect("value"),
        )]),
        remove: Vec::new(),
        expected_attributes_revision_no: None,
    };
    let cases =
        [
            (
                CommitCandidate::new(create_directory_request("unassigned", "unassigned"))
                    .with_merge(merge(3, 7), FORK_POINT, BTreeMap::new()),
                "a merge operation that creates an inode should have an assigned inode ID",
            ),
            (
                CommitCandidate::new(intent("misassigned", vec![update])).with_merge(
                    merge(3, 7),
                    FORK_POINT,
                    BTreeMap::from([(0, fork_inode_id())]),
                ),
                "a merge's operation 0 should create the inode that the merge assigned it",
            ),
        ];
    for (candidate, message) in cases {
        for attempt in ["first", "again"] {
            let error = publish(&store, &namespace_id, &writer, candidate.clone())
                .await
                .expect_err(attempt);
            assert_eq!(error.code(), ErrorCode::ServerError, "{attempt}");
            assert!(!error.code().is_stale_plan(), "{attempt}");
            assert_matches!(
                &error,
                CoreError::CommitPreparationPanicked(panic) if panic.contains(message),
                "{attempt}"
            );
        }
    }
    let after = publish(&store, &namespace_id, &writer, create_dir("after", "after"))
        .await
        .expect("a commit after the merges");
    assert_eq!(feed(&store, &namespace_id).await, [directory, after]);
}

/// A subject with `principals`.
fn acl_subject(principals: &[&str]) -> loonfs_types::Subject {
    loonfs_types::Subject {
        principals: loonfs_types::PrincipalSet::new(
            principals
                .iter()
                .map(|id| loonfs_types::PrincipalId::parse(id).expect("principal id"))
                .collect(),
        )
        .expect("principals"),
    }
}

fn acl_grants(principal: &str, rights: &[loonfs_types::AccessRight]) -> loonfs_types::AccessGrants {
    loonfs_types::AccessGrants::new(BTreeMap::from([(
        loonfs_types::PrincipalId::parse(principal).expect("principal"),
        loonfs_types::AccessRights::from_iter(rights.iter().copied()),
    )]))
    .expect("grants")
}

/// Checks that the subject's rights are checked against a merge's net
/// change, and not against the temporary directory of a move that a
/// temporary name splits in two. `prn_team` can create and remove entries in
/// `/x` and `/y`, and has no right on `/t`. A swap of `/x/a` and `/y/b` whose
/// first move goes through a temporary name in `/t` commits when the merge
/// marks the two legs, and is refused when it doesn't. The swapped entries
/// are directories, which need no content.
#[tokio::test]
async fn a_swap_through_a_temporary_name_needs_no_right_on_the_temporary_directory() {
    use loonfs_types::AccessRight;
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let writer = context("writer");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &writer);
    fixture.create().await.expect("create");
    fixture
        .set_root_access(acl_grants("prn_root", &[AccessRight::Admin]))
        .await
        .expect("the root access row");
    let path = |path: &str| AbsolutePath::parse(path).expect("path");
    let team = acl_grants(
        "prn_team",
        &[AccessRight::Read, AccessRight::Create, AccessRight::Remove],
    );
    let mut operations: Vec<_> = ["/x", "/y", "/t"]
        .map(|directory| FilesystemOperation::CreateDirectory {
            path: path(directory),
            parents: false,
        })
        .into();
    for (directory, boundary, grants) in [
        ("/x", false, team.clone()),
        ("/y", false, team.clone()),
        ("/t", true, acl_grants("prn_root", &[AccessRight::Read])),
    ] {
        operations.push(FilesystemOperation::UpdateAccess {
            path: path(directory),
            boundary,
            grants,
            expected_inode_id: None,
            expected_access_revision_no: None,
        });
    }
    let root = acl_subject(&["prn_root"]);
    publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(intent("layout", operations).with_subject(root.clone())),
    )
    .await
    .expect("the layout");
    for file in ["/x/a", "/y/b"] {
        publish(
            &store,
            &namespace_id,
            &writer,
            CommitCandidate::new(
                intent(
                    &format!("create-{}", &file[1..2]),
                    vec![FilesystemOperation::CreateDirectory {
                        path: path(file),
                        parents: false,
                    }],
                )
                .with_subject(root.clone()),
            ),
        )
        .await
        .expect("the entry");
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let entry = |path: &'static str| {
        let view = &view;
        async move {
            view.resolve_path(
                path,
                loonfs_types::options::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Service),
            )
            .await
            .expect("entry")
        }
    };
    let (a, b, x, t) = (
        entry("/x/a").await,
        entry("/y/b").await,
        entry("/x").await,
        entry("/t").await,
    );
    let temporary = DisplayName::parse(a.inode_id.to_string()).expect("name");
    let swap = |commit_id: &str| {
        CommitCandidate::new(
            intent(
                commit_id,
                vec![
                    FilesystemOperation::MoveByInode {
                        inode_id: a.inode_id,
                        expected_binding_version: a.binding_version.clone().expect("version"),
                        destination_parent_inode_id: t.inode_id,
                        destination_display_name: temporary.clone(),
                        precondition: Default::default(),
                    },
                    FilesystemOperation::MoveByInode {
                        inode_id: b.inode_id,
                        expected_binding_version: b.binding_version.clone().expect("version"),
                        destination_parent_inode_id: x.inode_id,
                        destination_display_name: DisplayName::parse("a").expect("name"),
                        precondition: Default::default(),
                    },
                    FilesystemOperation::MovePath {
                        source_path: path(&format!("/t/{temporary}")),
                        destination_path: path("/y/b"),
                        precondition: Default::default(),
                    },
                ],
            )
            .with_subject(acl_subject(&["prn_team"])),
        )
        .with_merge(merge(3, 7), FORK_POINT, BTreeMap::new())
    };
    let error = publish(&store, &namespace_id, &writer, swap("unmarked"))
        .await
        .expect_err("the temporary directory refuses an unmarked leg");
    assert!(
        matches!(
            error.code(),
            ErrorCode::Forbidden | ErrorCode::InodeNotFound | ErrorCode::PathNotFound
        ),
        "{error}"
    );
    let marked = swap("marked").with_temporary_moves(BTreeMap::from([
        (0, crate::commit::engine::TemporaryMove::Away),
        (
            2,
            crate::commit::engine::TemporaryMove::Back {
                from_parent: x.inode_id,
            },
        ),
    ]));
    publish(&store, &namespace_id, &writer, marked)
        .await
        .expect("the swap commits");
    let view = namespace.load_current_metadata_view().await.expect("view");
    for (path, inode_id) in [("/x/a", b.inode_id), ("/y/b", a.inode_id)] {
        let moved = view
            .resolve_path(
                path,
                loonfs_types::options::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Service),
            )
            .await
            .expect("entry");
        assert_eq!(moved.inode_id, inode_id, "{path}");
    }
}
