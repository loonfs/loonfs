//! A rebase's new fork: the `rebased_from` that its creation records through
//! its fork pin, and the two substitutions in the merge rules for a replay
//! commit into it.

use super::*;
use loonfs_objectstore::keys::pin_prefix;
use loonfs_types::NamespaceRebasedFrom;

fn rebase_fork(name: &str) -> NamespaceId {
    NamespaceId::parse(name).expect("namespace")
}

/// Commits ordinary commits until `main`'s head reaches the fork point.
async fn reach_fork_point(store: &LocalFsStore, main: &NamespaceId, writer: &WriterAt) {
    let mut head = ChangeSeq(0);
    while head < FORK_POINT {
        let name = format!("before-{}", head.0);
        let candidate = CommitCandidate::new(create_dir_request(&name, &name));
        head = publish(store, main, writer, candidate)
            .await
            .expect("an ordinary commit")
            .committed_seq;
    }
}

/// Makes `target` a rebase's new fork of `main`'s head, for the tests' fork.
async fn fork_for_rebase(
    store: &LocalFsStore,
    main: &NamespaceId,
    writer: &WriterAt,
    target: &NamespaceId,
) -> Result<loonfs_types::NamespaceMetadata> {
    let fixture = NamespaceFixture::new(store, main, writer);
    fixture
        .engine()
        .fork_namespace_for_rebase(
            target,
            &loonfs_test_support::test_actor(),
            &fork(),
            FORK_POINT,
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
}

/// Checks that a rebase's new fork records the fork sequence that the fork's
/// next merge into main starts after, as main's pinned manifest records it,
/// and that a fork point below that manifest's retention floor creates no
/// namespace and leaves no fork pin.
#[tokio::test]
async fn a_rebase_fork_records_where_the_forks_next_merge_starts() {
    let (_directory, store, main, writer) = setup().await;
    reach_fork_point(&store, &main, &writer).await;
    let unmerged = fork_for_rebase(&store, &main, &writer, &rebase_fork("unmerged"))
        .await
        .expect("a rebase fork before the fork's first merge");
    publish(
        &store,
        &main,
        &writer,
        merge_candidate("first", "first", merge(3, 7)),
    )
    .await
    .expect("the first merge");
    let merged = fork_for_rebase(&store, &main, &writer, &rebase_fork("merged"))
        .await
        .expect("a rebase fork after the first merge");
    for (created, resume_after_seq) in [(&unmerged, FORK_POINT), (&merged, ChangeSeq(7))] {
        assert_eq!(
            created
                .fork_basis
                .as_ref()
                .and_then(|basis| basis.rebased_from.clone()),
            Some(NamespaceRebasedFrom {
                source_namespace_id: fork(),
                resume_after_seq,
            })
        );
        let manifest = crate::test_support::namespace_store(&store, &created.namespace_id)
            .load_current_manifest()
            .await
            .expect("the rebase fork's manifest");
        let rebased_from = manifest
            .payload()
            .fork_basis
            .as_ref()
            .and_then(|basis| basis.rebased_from.as_ref())
            .expect("recorded");
        assert_eq!(rebased_from.resume_after_seq, resume_after_seq);
    }

    let floor = advance_floor_and_compact(&store, &main, RetentionTarget::Head).await;
    let refused = rebase_fork("refused");
    let error = fork_for_rebase(&store, &main, &writer, &refused)
        .await
        .expect_err("the fork point is below main's floor");
    assert_matches!(
        error,
        CoreError::RebootstrapRequired {
            below: crate::error::HistoryPosition::ForkPoint { fork_point_seq },
            retention_floor_seq,
        } if fork_point_seq == FORK_POINT && retention_floor_seq == floor
    );
    assert_matches!(
        crate::test_support::namespace_store(&store, &refused)
            .load_current_manifest()
            .await,
        Err(CoreError::NamespaceNotFound { .. })
    );
    assert_eq!(
        store
            .list_prefix(&pin_prefix(&main))
            .await
            .expect("main's pins")
            .len(),
        2,
        "only the two rebase forks keep fork pins"
    );
}

/// Checks the two substitutions in the merge rules for a replay commit into
/// a rebase's new fork. A compaction of the new fork removes the merge index
/// rows that it inherited from main, and the range rule still starts the
/// first replay commit after `resume_after_seq`, where without the
/// substitution it would expect the fork point and admit a replay of the
/// fork's merged commits. The floor rule compares the new fork's own fork
/// point with its floor, so it admits replay commits while the floor is
/// above the fork's fork point, and refuses them once the floor passes the
/// new fork's own fork point.
#[tokio::test]
async fn a_replay_commit_reads_the_rebase_forks_own_fork_point_and_resume_after_seq() {
    let (_directory, store, main, writer) = setup().await;
    reach_fork_point(&store, &main, &writer).await;
    publish(
        &store,
        &main,
        &writer,
        merge_candidate("first", "first", merge(3, 7)),
    )
    .await
    .expect("the first merge");
    publish(
        &store,
        &main,
        &writer,
        CommitCandidate::new(create_dir_request("later", "later")),
    )
    .await
    .expect("a commit after the merge");
    let rebase = rebase_fork("rebase");
    let own_fork_point = fork_for_rebase(&store, &main, &writer, &rebase)
        .await
        .expect("the rebase fork")
        .head_seq;
    // The new fork inherits main's base run, and its own commit gives the
    // compaction a delta run to fold into a new base.
    publish(
        &store,
        &rebase,
        &writer,
        CommitCandidate::new(create_dir_request("own", "own")),
    )
    .await
    .expect("a commit in the rebase fork");
    let floor =
        advance_floor_and_compact(&store, &rebase, RetentionTarget::Seq(own_fork_point)).await;
    assert_eq!(floor, own_fork_point);
    assert!(FORK_POINT < floor);
    let inherited = crate::test_support::namespace_store(&store, &rebase)
        .load_current_metadata_view()
        .await
        .expect("read view")
        .metadata_view()
        .newest_merge(&fork())
        .await
        .expect("merge lookup");
    assert_eq!(inherited, None, "the compaction removes the inherited rows");

    let error = publish(
        &store,
        &rebase,
        &writer,
        merge_candidate("again", "again", merge(3, 9)),
    )
    .await
    .expect_err("a replay of the fork's merged commits");
    assert_range_conflict(error, 7, 3);
    publish(
        &store,
        &rebase,
        &writer,
        merge_candidate("replay", "replay", merge(7, 9)),
    )
    .await
    .expect("the first replay commit starts after resume_after_seq");
    let error = publish(
        &store,
        &rebase,
        &writer,
        merge_candidate("overlap", "overlap", merge(7, 12)),
    )
    .await
    .expect_err("a replay that overlaps the first");
    assert_range_conflict(error, 9, 7);

    let floor = advance_floor_and_compact(&store, &rebase, RetentionTarget::Head).await;
    assert!(own_fork_point < floor);
    let error = publish(
        &store,
        &rebase,
        &writer,
        merge_candidate("next", "next", merge(9, 12)),
    )
    .await
    .expect_err("the floor passed the rebase fork's own fork point");
    assert_matches!(
        error,
        CoreError::RebootstrapRequired {
            below: crate::error::HistoryPosition::ForkPoint { fork_point_seq },
            retention_floor_seq,
        } if fork_point_seq == own_fork_point && retention_floor_seq == floor
    );
}
