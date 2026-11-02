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
use loonfs_types::format::wal::LandRecord;
use loonfs_types::{CompactorEpoch, EffectiveLimit, Land};

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

/// A land of `land` under `commit_id` that creates `/{name}`.
fn land_candidate(commit_id: &str, name: &str, land: LandRecord) -> CommitCandidate {
    CommitCandidate::new(create_dir_request(commit_id, name)).with_land(land, FORK_POINT)
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
    list_changes_after(&view, ChangeSeq(0), EffectiveLimit::MAX)
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

/// Checks that each land in one batch is validated against the batch's
/// earlier lands, so a second submission of a range in the same batch is
/// refused, and a land that continues it is admitted.
#[tokio::test]
async fn lands_in_one_batch_continue_each_other() {
    let (_directory, store, namespace_id, writer) = setup().await;
    let mut results = NamespaceFixture::new(&store, &namespace_id, &writer)
        .publish_candidates(vec![
            land_candidate("first", "first", land(3, 7)),
            land_candidate("again", "again", land(3, 7)),
            land_candidate("next", "next", land(7, 9)),
        ])
        .await
        .into_iter();
    let first = results.next().expect("first").expect("the first land");
    let again = results.next().expect("second");
    assert_range_conflict(again.expect_err("the same range again"), 7, 3);
    let next = results.next().expect("third").expect("the next land");
    assert_eq!(next.committed_seq.0, first.committed_seq.0 + 1);
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
