//! Land commits: the land record in the WAL, the change feed, and the
//! commit rows, a land's retry identity, the range rule that makes each
//! land continue the previous land from its fork, and the refusal of a land
//! whose fork point is below the retention floor.

use super::*;
use crate::changes::list_changes_after;
use crate::compaction::{
    CompactionAdmission, CompactionUnitOutcome, MetadataCompactionCancellation,
    MetadataCompactionPolicy,
};
use crate::manifest::RetentionTarget;
use crate::test_support::NamespaceFixture;
use loonfs_types::format::wal::{LandRecord, MAX_WAL_OBJECT_BYTES};
use loonfs_types::{CompactorEpoch, EffectiveLimit, InodeId, Land};
use std::collections::BTreeMap;

/// The sequence of `main` that the tests' fork was made at.
const FORK_POINT: ChangeSeq = ChangeSeq(3);

fn fork() -> NamespaceId {
    NamespaceId::parse("main-fork").expect("namespace")
}

/// A land of the fork's commits above `after_seq`, through `through_seq`.
fn land(after_seq: u64, through_seq: u64) -> LandRecord {
    LandRecord {
        source_namespace_id: fork(),
        source_after_seq: ChangeSeq(after_seq),
        source_through_seq: ChangeSeq(through_seq),
    }
}

fn reported(land: &LandRecord) -> Land {
    Land {
        source_namespace_id: land.source_namespace_id.clone(),
        source_after_seq: land.source_after_seq,
        source_through_seq: land.source_through_seq,
    }
}

/// An ID that an inode created in the fork has.
fn fork_inode_id() -> InodeId {
    InodeId::generate(ChangeSeq(FORK_POINT.0 + 1), std::num::NonZeroU32::MIN)
}

/// A land of `land` under `commit_id` that creates `/{name}` under an inode
/// ID from the fork.
fn land_candidate(commit_id: &str, name: &str, land: LandRecord) -> CommitCandidate {
    CommitCandidate::new(create_dir_request(commit_id, name)).with_land(
        land,
        FORK_POINT,
        BTreeMap::from([(0, fork_inode_id())]),
    )
}

async fn setup() -> (tempfile::TempDir, LocalFsStore, NamespaceId, WriterAt) {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let writer = context("writer");
    NamespaceFixture::new(&store, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    (directory, store, namespace_id, writer)
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
        loonfs_types::ChangeFeedPosition::after(ChangeSeq(0)),
        EffectiveLimit::MAX,
        loonfs_types::options::CommitSplitting::Whole,
    )
    .await
    .expect("change feed")
    .changes
}

/// Checks that the land record reaches the commit response, the change feed
/// while the commit is in the WAL tail, and the change feed after a fold
/// writes the commit row, and that an ordinary commit reports none.
#[tokio::test]
async fn the_feed_reports_a_land_record_before_and_after_a_fold() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let ordinary = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(create_dir_request("ordinary", "ordinary")),
    )
    .await
    .expect("ordinary commit");
    assert_eq!(ordinary.land, None);
    let landed = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("land", "landed", land(3, 7)),
    )
    .await
    .expect("land");
    assert_eq!(landed.land, Some(reported(&land(3, 7))));

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
            [ordinary.clone(), landed.clone()],
            "folded: {folded}"
        );
    }
}

/// Checks that a land's identity is its land record: a retry with the same
/// commit ID and land record replays the original commit whatever its
/// operations, before and after a fold, and a different range conflicts.
#[tokio::test]
async fn a_reused_commit_id_replays_the_same_land_and_conflicts_with_another_range() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let original = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("land", "landed", land(3, 7)),
    )
    .await
    .expect("land");

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
            land_candidate("land", "planned-again", land(3, 7)),
        )
        .await
        .expect("replay");
        assert_eq!(replayed, original, "folded: {folded}");

        let error = publish(
            &store,
            &namespace_id,
            &writer,
            land_candidate("land", "landed", land(3, 8)),
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

/// Checks a refused land's error, its wire code, and its details.
fn assert_range_conflict(error: CoreError, expected_after_seq: u64, actual_after_seq: u64) {
    assert_eq!(error.code(), ErrorCode::LandRangeConflict);
    assert_eq!(error.code().kind(), loonfs_types::ErrorKind::Conflict);
    let details = error.details().expect("details");
    assert_eq!(details.source_namespace_id, Some(fork()));
    assert_eq!(
        details.expected_after_seq,
        Some(ChangeSeq(expected_after_seq))
    );
    assert_eq!(details.actual_after_seq, Some(ChangeSeq(actual_after_seq)));
    assert_matches!(
        error,
        CoreError::LandRangeConflict { source_namespace_id, .. } if source_namespace_id == fork()
    );
}

/// Checks that a fork's first land has to start at the fork point.
#[tokio::test]
async fn a_first_land_starts_at_the_fork_point() {
    let (_directory, store, namespace_id, writer) = setup().await;
    for after_seq in [2, 4] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            land_candidate("early-or-late", "skipped", land(after_seq, 7)),
        )
        .await
        .expect_err("a first land that doesn't start at the fork point");
        assert_range_conflict(error, FORK_POINT.0, after_seq);
    }
    let landed = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("first", "first", land(FORK_POINT.0, 7)),
    )
    .await
    .expect("a first land at the fork point");
    assert_eq!(landed.land, Some(reported(&land(FORK_POINT.0, 7))));
}

/// Checks that a repeat land has to start where the previous land stopped,
/// whether the previous land's index row is in a folded run or in the WAL
/// tail, and that a retry of the previous land with its original commit ID
/// still replays, because the receipt check runs before the range rule.
#[tokio::test]
async fn a_repeat_land_continues_the_previous_land_exactly() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let first = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("first", "first", land(3, 7)),
    )
    .await
    .expect("first land");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the first land");

    for (after_seq, why) in [(5, "an overlap"), (8, "a gap")] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            land_candidate("repeat", "repeat", land(after_seq, 9)),
        )
        .await
        .expect_err(why);
        assert_range_conflict(error, 7, after_seq);
    }
    publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("repeat", "repeat", land(7, 9)),
    )
    .await
    .expect("a land that continues the first");
    let error = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("third", "third", land(7, 12)),
    )
    .await
    .expect_err("a land that repeats the land in the WAL tail");
    assert_range_conflict(error, 9, 7);

    let replayed = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("first", "planned-again", land(3, 7)),
    )
    .await
    .expect("a retry of the first land");
    assert_eq!(replayed, first);
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

/// Checks that a land isn't held to the operation count that limits a
/// request body, and that its creates make inodes under the fork's IDs.
#[tokio::test]
async fn a_land_of_ten_thousand_creates_is_admitted_where_an_explicit_commit_is_refused() {
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
    let landed = publish(
        &store,
        &namespace_id,
        &writer,
        CommitCandidate::new(creates).with_land(land(3, 7), FORK_POINT, fork_ids.clone()),
    )
    .await
    .expect("a land of the same creates");
    let created: Vec<InodeId> = landed
        .events
        .iter()
        .map(|event| match event {
            loonfs_types::FilesystemChange::DirectoryCreated { inode_id, .. } => *inode_id,
            other => panic!("expected a directory creation, got {other:?}"),
        })
        .collect();
    assert_eq!(created, fork_ids.into_values().collect::<Vec<_>>());
}

/// Checks that a land whose planned record doesn't fit in one WAL object
/// fails with `land_too_large`, which has the 413 kind, and writes no WAL
/// object.
#[tokio::test]
async fn a_land_over_the_wal_object_bound_fails_with_land_too_large_and_writes_nothing() {
    let (_directory, store, namespace_id, writer) = setup().await;
    // One engine publishes both commits, so the land's publication acquires
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
    let loonfs_types::FilesystemChange::DirectoryCreated { inode_id, .. } = directory.events[0]
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
        CommitCandidate::new(intent("too-large", operations)).with_land(
            land(3, 7),
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
        .expect_err("a land over the bound");
    assert_matches!(
        error,
        CoreError::LandTooLarge { estimated_bytes, max_bytes }
            if estimated_bytes > max_bytes && max_bytes == MAX_WAL_OBJECT_BYTES
    );
    assert_eq!(error.code(), ErrorCode::LandTooLarge);
    assert_eq!(
        error.code().kind(),
        loonfs_types::ErrorKind::ContentTooLarge
    );
    assert_eq!(wal_object_count(&store, &namespace_id).await, before);
}

/// Checks that a land refers only to content that the fork already wrote:
/// inline content and appended bytes are refused.
#[tokio::test]
async fn a_land_with_inline_content_or_an_append_is_refused() {
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
            content: loonfs_types::FileContent::Ref {
                content_ref: inline.content_ref().clone(),
                source_namespace_id: None,
            },
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
        CommitCandidate::with_inline_content(put, Vec::new(), vec![inline]).with_land(
            land(3, 7),
            FORK_POINT,
            BTreeMap::from([(0, fork_inode_id())]),
        ),
        CommitCandidate::new(append).with_land(land(3, 7), FORK_POINT, BTreeMap::new()),
    ] {
        let error = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect_err("a land with bytes of its own");
        assert_matches!(
            error,
            CoreError::InvalidCommitRequest(message)
                if message == "a land includes no inline content or appended bytes"
        );
    }
}

/// A file that a fork created, landed into the namespace it was forked
/// from.
struct LandedFile {
    store: RecordingStore<LocalFsStore>,
    main: NamespaceId,
    fork: NamespaceId,
    inode_id: InodeId,
    bytes: Vec<u8>,
}

/// Forks `main`, writes a 1 MiB file in the fork, folds the fork, and lands
/// the file into `main` the way the land planner does: a create under the
/// fork's inode ID, with the reference and layout that the fork's rows
/// record. The store records content requests, and its counters are reset
/// just before the land.
async fn land_a_forks_file() -> (tempfile::TempDir, LandedFile) {
    let directory = tempdir().expect("tempdir");
    let [main, fork] = ["main", "main-fork"].map(|name| NamespaceId::parse(name).expect("ns"));
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::content_prefix(&main)),
    );
    let writer = context("writer");
    let main_fixture = NamespaceFixture::new(&store, &main, &writer);
    main_fixture.create().await.expect("create main");
    main_fixture
        .engine()
        .fork_namespace(&fork, &loonfs_test_support::test_actor(), None)
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
    let loonfs_types::FilesystemChange::FileCreated {
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
        "land-big",
        vec![FilesystemOperation::CreateFileByInode {
            parent_inode_id: loonfs_types::ROOT_INODE_ID,
            display_name: loonfs_types::DisplayName::parse("big.bin").expect("name"),
            content: loonfs_types::FileContent::Ref {
                content_ref: content_ref.clone(),
                source_namespace_id: None,
            },
        }],
    );
    let proof = PreparedContent::for_family_member(main.clone(), content_ref, layout, u64::MAX);
    let candidate = CommitCandidate::prepared(create, vec![proof]).with_land(
        LandRecord {
            source_namespace_id: fork.clone(),
            source_after_seq: fork_point_seq,
            source_through_seq: written.committed_seq,
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
        .expect("land the file");
    let landed = LandedFile {
        store,
        main,
        fork,
        inode_id,
        bytes,
    };
    (directory, landed)
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
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("stat by inode ID")
}

/// Checks that a land admits a fork's file from the reference and layout
/// that the fork's rows record, without reading the file's bytes. The test
/// store caches no content, so a read of the bytes would reach the store, as
/// the read after the land does.
#[tokio::test]
async fn a_land_of_a_forks_file_reads_none_of_its_bytes() {
    let (_directory, landed) = land_a_forks_file().await;
    assert_eq!(landed.store.count(OperationSelector::Any), 0);

    let read = crate::test_support::namespace_store(&landed.store, &landed.main)
        .load_current_metadata_view()
        .await
        .expect("view")
        .read_file_bytes_by_inode(
            landed.inode_id,
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("read the landed file");
    assert_eq!(read.bytes, landed.bytes);
    assert!(landed.store.count(OperationSelector::Read) > 0);
}

/// Checks that a land creates a fork's inode under the fork's ID, so a stat
/// by that ID finds the same file in both namespaces.
#[tokio::test]
async fn a_land_creates_the_forks_inode_under_the_forks_id() {
    let (_directory, landed) = land_a_forks_file().await;
    let in_main = stat(&landed.store, &landed.main, landed.inode_id).await;
    let in_fork = stat(&landed.store, &landed.fork, landed.inode_id).await;
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
) -> ChangeSeq {
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let maintainer = NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "maintainer",
            crate::test_support::grace_elapsed_after(1_000),
        ),
    );
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
                MetadataCompactionPolicy::CompactImmediately,
                CompactionAdmission::AnySize,
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

/// Checks that a land is admitted while the retention floor is at its fork
/// point, and refused with `rebootstrap_required` once the floor is above
/// the fork point. By then a compaction has removed the fork's land index
/// rows, so without the refusal the writer would expect the next land to
/// start at the fork point, and it would admit a land that applies the
/// fork's landed commits again.
#[tokio::test]
async fn a_land_whose_fork_point_is_below_the_retention_floor_is_refused() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let mut head = ChangeSeq(0);
    while head < FORK_POINT {
        let name = format!("before-{}", head.0);
        let candidate = CommitCandidate::new(create_dir_request(&name, &name));
        head = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect("an ordinary commit")
            .committed_seq;
    }
    let first = land_candidate("first", "first", land(3, 7));
    publish(&store, &namespace_id, &writer, first)
        .await
        .expect("the first land");
    let floor =
        advance_floor_and_compact(&store, &namespace_id, RetentionTarget::Seq(FORK_POINT)).await;
    assert_eq!(floor, FORK_POINT);
    let repeat = land_candidate("repeat", "repeat", land(7, 9));
    publish(&store, &namespace_id, &writer, repeat)
        .await
        .expect("a land whose fork point is at the floor");

    let after = CommitCandidate::new(create_dir_request("after", "after"));
    publish(&store, &namespace_id, &writer, after)
        .await
        .expect("an ordinary commit after the lands");
    let floor = advance_floor_and_compact(&store, &namespace_id, RetentionTarget::Head).await;
    let index = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .newest_land(&fork())
        .await
        .expect("land lookup");
    assert_eq!(index, None, "the compaction removes the fork's land rows");

    for after_seq in [FORK_POINT.0, 9] {
        let error = publish(
            &store,
            &namespace_id,
            &writer,
            land_candidate("third", "third", land(after_seq, 12)),
        )
        .await
        .expect_err("a land whose fork point is below the floor");
        assert_eq!(error.code(), ErrorCode::RebootstrapRequired);
        let details = error.details().expect("details");
        assert_eq!(details.fork_point_seq, Some(FORK_POINT));
        assert_eq!(details.retention_floor_seq, Some(floor));
        assert_matches!(
            error,
            CoreError::LandRebootstrapRequired { fork_point_seq, retention_floor_seq }
                if fork_point_seq == FORK_POINT && retention_floor_seq == floor
        );
    }
}

/// The inode that a commit's first event created.
fn created_inode(commit: &Commit) -> InodeId {
    match &commit.events[0] {
        loonfs_types::FilesystemChange::DirectoryCreated { inode_id, .. } => *inode_id,
        other => panic!("expected a directory creation, got {other:?}"),
    }
}

/// Checks that a land failed at `operation_index` with `InvalidCommitRequest`
/// and `message`.
fn assert_refused_land(error: CoreError, operation_index: u32, message: &str) {
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_matches!(
        error,
        CoreError::FailedOperation { operation_index: index, error }
            if index == operation_index
                && matches!(&*error, CoreError::InvalidCommitRequest(actual) if actual == message)
    );
}

/// Checks that a land can't create an inode under an ID that the namespace
/// already has, whether an ordinary commit or an earlier land created it, or
/// under one ID twice, and that a refused land publishes nothing.
#[tokio::test]
async fn a_land_refuses_an_inode_id_that_already_exists() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let ordinary = publish(
        &store,
        &namespace_id,
        &writer,
        create_dir("ordinary", "ordinary"),
    )
    .await
    .expect("an ordinary commit");
    let landed = publish(
        &store,
        &namespace_id,
        &writer,
        land_candidate("first", "first", land(3, 7)),
    )
    .await
    .expect("the first land");
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
        (vec![(0, created_inode(&landed))], 0),
        (vec![(0, repeated), (1, repeated)], 1),
    ];
    for (assigned, refused_index) in cases {
        let inode_id = assigned[refused_index].1;
        let candidate = CommitCandidate::new(intent("again", creates(&["one", "two"]))).with_land(
            land(7, 9),
            FORK_POINT,
            assigned.into_iter().collect(),
        );
        let error = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect_err("a land of an inode ID that exists");
        assert_refused_land(
            error,
            u32::try_from(refused_index).expect("index"),
            &format!("a land creates inode `{inode_id}`, which already exists"),
        );
    }
    assert_eq!(feed(&store, &namespace_id).await, [ordinary, landed]);
}

/// Checks that a land whose inode IDs don't match its creates fails instead
/// of panicking the publisher: a create without an ID, and an ID for an
/// operation that creates no inode.
#[tokio::test]
async fn a_land_whose_inode_ids_dont_match_its_creates_is_refused() {
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
    let assigned = fork_inode_id();
    let cases = [
        (
            CommitCandidate::new(create_dir_request("unassigned", "unassigned")).with_land(
                land(3, 7),
                FORK_POINT,
                BTreeMap::new(),
            ),
            "a land operation creates an inode but has no assigned inode ID".to_owned(),
        ),
        (
            CommitCandidate::new(intent("misassigned", vec![update])).with_land(
                land(3, 7),
                FORK_POINT,
                BTreeMap::from([(0, assigned)]),
            ),
            format!("a land assigned inode `{assigned}` to an operation that creates no inode"),
        ),
    ];
    for (candidate, message) in cases {
        let error = publish(&store, &namespace_id, &writer, candidate)
            .await
            .expect_err("a land whose IDs don't match its creates");
        assert_refused_land(error, 0, &message);
    }
    assert_eq!(feed(&store, &namespace_id).await, [directory]);
}
