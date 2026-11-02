//! Land commits: the land record in the WAL, the change feed, and the
//! commit rows, and a land's retry identity.

use super::*;
use crate::changes::list_changes_after;
use crate::test_support::NamespaceFixture;
use loonfs_types::format::wal::LandRecord;
use loonfs_types::{EffectiveLimit, Land};

fn land(through_seq: u64) -> LandRecord {
    LandRecord {
        source_namespace_id: NamespaceId::parse("main-fork").expect("namespace"),
        source_after_seq: ChangeSeq(3),
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
    CommitCandidate::new(create_dir_request(commit_id, name)).with_land(land)
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
        land_candidate("land", "landed", land(7)),
    )
    .await
    .expect("land");
    assert_eq!(landed.land, Some(reported(&land(7))));

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
        land_candidate("land", "landed", land(7)),
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
            land_candidate("land", "planned-again", land(7)),
        )
        .await
        .expect("replay");
        assert_eq!(replayed, original, "folded: {folded}");

        let error = publish(
            &store,
            &namespace_id,
            &writer,
            land_candidate("land", "landed", land(8)),
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
