//! A fork's grep index: the fork inherits its source's entry at the fork
//! point, reads the source's commits that the inherited index doesn't cover
//! through the fork snapshot, restarts an inherited backfill in the fork, and
//! clears an inherited reorganization.

use crate::common::control;
use crate::content_index::{request, ContentFixture};
use loonfs::{Namespace, NamespaceId, Writable};
use loonfs_grep::entry::{GrepEntry, GrepIndexStatus};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepPublishOutcome, GrepReorganizeOutcome,
    NamespaceReads,
};
use loonfs_test_support::ids::{first_page, nonzero_usize};
use loonfs_types::GrepResponse;
use std::assert_matches;
use std::collections::BTreeSet;

/// Forks the fixture's namespace at its head as `{name}-fork`, through the
/// grep host's runtime, and returns the fork's ID and a writable handle on
/// it.
async fn fork(fixture: &ContentFixture) -> (NamespaceId, Namespace<Writable>) {
    let fork_id =
        NamespaceId::parse(format!("{}-fork", fixture.namespace_id)).expect("namespace id");
    fixture
        .host
        .writer
        .fork_namespace(
            &fixture.namespace_id,
            &fork_id,
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("fork");
    let handle = fixture
        .host
        .writer
        .open_namespace(&fork_id)
        .expect("open the fork");
    (fork_id, handle)
}

async fn entry(fixture: &ContentFixture, namespace_id: &NamespaceId) -> GrepEntry {
    control::grep_entry(&fixture.store, namespace_id)
        .await
        .expect("grep's entry exists")
}

async fn grep(fixture: &ContentFixture, namespace_id: &NamespaceId, pattern: &str) -> GrepResponse {
    fixture
        .host
        .grep(namespace_id, &request(pattern), first_page())
        .await
        .expect("grep")
}

fn paths(response: &GrepResponse) -> BTreeSet<String> {
    response
        .matches
        .iter()
        .map(|found| found.path.as_str().to_owned())
        .collect()
}

/// Checks that a fork of a namespace with an active index answers a query at
/// once from the inherited index, whose segments the source owns, and that
/// the fork's first build step indexes only the fork's own commit.
#[tokio::test]
async fn a_fork_answers_from_the_inherited_index_before_any_step() {
    let fixture = ContentFixture::new("inherit-active").await;
    fixture.put("/a.txt", b"inherited needle\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let source_entry = entry(&fixture, &fixture.namespace_id).await;

    let (fork_id, fork_handle) = fork(&fixture).await;
    let inherited = entry(&fixture, &fork_id).await;
    assert_eq!(
        inherited, source_entry,
        "the fork copies the source's entry"
    );
    assert!(inherited
        .segments()
        .all(|segment| segment.owner_namespace_id == fixture.namespace_id));
    let found = grep(&fixture, &fork_id, "inherited needle").await;
    assert_eq!(paths(&found), BTreeSet::from(["/a.txt".to_owned()]));
    assert_eq!(found.built_through_seq, found.head_seq);

    fork_handle
        .put_file(
            "/b.txt",
            b"inherited needle in the fork\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write the fork");
    assert_eq!(
        paths(&grep(&fixture, &fork_id, "inherited needle").await),
        BTreeSet::from(["/a.txt".to_owned(), "/b.txt".to_owned()]),
        "the fork's own commit is the index's unindexed tail"
    );
    assert_matches!(
        fixture
            .host
            .worker
            .steps(&fork_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await,
        Ok(GrepBuildOutcome::Indexed {
            indexed_revisions: 1,
            ..
        })
    );
    assert_eq!(
        paths(&grep(&fixture, &fixture.namespace_id, "inherited needle").await),
        BTreeSet::from(["/a.txt".to_owned()]),
        "the source doesn't see the fork's file"
    );
}

/// Checks that a fork whose inherited watermark is below the fork point
/// reads the source's commits between the two through the fork snapshot, in a
/// query's tail and in the fork's first build step, after the source's
/// retention floor has passed the fork point.
#[tokio::test]
async fn the_gap_below_the_fork_point_is_read_through_the_fork_snapshot() {
    let fixture = ContentFixture::new("fork-gap").await;
    fixture.put("/a.txt", b"indexed needle\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let gap = fixture.put("/gap.txt", b"gap needle\n").await.committed_seq;

    let (fork_id, _fork_handle) = fork(&fixture).await;
    fixture.put("/after.txt", b"after the fork\n").await;
    let floor = fixture.advance_retention_floor().await;
    assert!(floor > gap, "the source's floor passes the fork point");
    let GrepIndexStatus::Active {
        built_through_seq, ..
    } = entry(&fixture, &fork_id).await.status().clone()
    else {
        panic!("the fork inherits an active index");
    };
    assert!(
        built_through_seq < gap,
        "the inherited index lags the fork point"
    );

    let tail = grep(&fixture, &fork_id, "gap needle").await;
    assert_eq!(paths(&tail), BTreeSet::from(["/gap.txt".to_owned()]));
    assert!(tail.built_through_seq < tail.head_seq);

    let mut indexed = 0;
    loop {
        match fixture
            .host
            .worker
            .steps(&fork_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await
            .expect("fork build step")
        {
            GrepBuildOutcome::Indexed {
                indexed_revisions, ..
            } => indexed += indexed_revisions,
            GrepBuildOutcome::UpToDate { built_through_seq } => {
                assert_eq!(built_through_seq, gap);
                break;
            }
            other => panic!("the gap is readable through the snapshot, not {other:?}"),
        }
    }
    assert_eq!(indexed, 1, "the gap's commit, and nothing before it");
    let found = grep(&fixture, &fork_id, "needle").await;
    assert_eq!(
        paths(&found),
        BTreeSet::from(["/a.txt".to_owned(), "/gap.txt".to_owned()])
    );
    assert_eq!(found.built_through_seq, found.head_seq);
}

/// Checks that a fork with commits of its own and an inherited watermark
/// below the fork point reads the gap through the fork snapshot and then its own
/// feed, in a query's tail and in the fork's build steps, which end up to
/// date at the fork's head.
#[tokio::test]
async fn a_fork_reads_the_gap_and_then_its_own_commits() {
    let fixture = ContentFixture::new("fork-gap-and-own").await;
    fixture.put("/a.txt", b"indexed needle\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let gap = fixture
        .put("/gap.txt", b"tail needle in the gap\n")
        .await
        .committed_seq;

    let (fork_id, fork_handle) = fork(&fixture).await;
    let own = fork_handle
        .put_file(
            "/own.txt",
            b"tail needle in the fork\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write the fork")
        .committed_seq;
    assert!(gap < own);
    let GrepIndexStatus::Active {
        built_through_seq, ..
    } = entry(&fixture, &fork_id).await.status().clone()
    else {
        panic!("the fork inherits an active index");
    };
    assert!(
        built_through_seq < gap,
        "the inherited index lags the fork point"
    );
    let gap_page = NamespaceReads::new(fixture.host.reader.clone(), &fork_id)
        .list_changes_after(built_through_seq, 100)
        .await
        .expect("the gap's page");
    assert_eq!(
        gap_page
            .changes
            .iter()
            .map(|change| change.committed_seq)
            .collect::<Vec<_>>(),
        [gap]
    );
    assert_eq!(
        gap_page.next_after_seq,
        Some(gap),
        "the fork's own feed continues above the fork point"
    );

    let tail = grep(&fixture, &fork_id, "tail needle").await;
    assert_eq!(
        paths(&tail),
        BTreeSet::from(["/gap.txt".to_owned(), "/own.txt".to_owned()])
    );
    assert_eq!(tail.head_seq, own);

    let mut indexed = 0;
    loop {
        match fixture
            .host
            .worker
            .steps(&fork_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await
            .expect("fork build step")
        {
            GrepBuildOutcome::Indexed {
                indexed_revisions, ..
            } => indexed += indexed_revisions,
            GrepBuildOutcome::UpToDate { built_through_seq } => {
                assert_eq!(built_through_seq, own, "up to date at the fork's head");
                break;
            }
            other => panic!("the gap is readable through the snapshot, not {other:?}"),
        }
    }
    assert_eq!(indexed, 2, "the gap's commit and the fork's own commit");
    let found = grep(&fixture, &fork_id, "needle").await;
    assert_eq!(
        paths(&found),
        BTreeSet::from([
            "/a.txt".to_owned(),
            "/gap.txt".to_owned(),
            "/own.txt".to_owned()
        ])
    );
    assert_eq!(found.built_through_seq, found.head_seq);
}

/// Checks that a fork restarts a backfill that it inherited, from a snapshot on
/// its own head, with the inherited segments, so the restarted walk reads
/// only the values that the inherited segments don't record.
#[tokio::test]
async fn a_fork_restarts_an_inherited_backfill_and_keeps_its_segments() {
    let fixture = ContentFixture::new("inherit-backfill").await;
    for path in ["/a.txt", "/b.txt", "/c.txt"] {
        fixture
            .put(path, format!("backfill needle {path}\n").as_bytes())
            .await;
    }
    fixture
        .host
        .worker
        .enable(&fixture.namespace_id)
        .await
        .expect("enable");
    let one_file = GramIndexBuildPolicy {
        max_files_per_step: nonzero_usize(1),
        ..GramIndexBuildPolicy::default()
    };
    fixture
        .host
        .worker
        .steps(&fixture.namespace_id, 0)
        .build_step(one_file)
        .await
        .expect("index one file");
    let source_entry = entry(&fixture, &fixture.namespace_id).await;
    let GrepIndexStatus::Backfilling {
        snapshot_id: source_snapshot_id,
        ..
    } = source_entry.status().clone()
    else {
        panic!("the source is mid-backfill");
    };
    assert!(!source_entry.runs().is_empty());

    let (fork_id, _fork_handle) = fork(&fixture).await;
    let fork_head = control::head(&fixture.store, &fork_id).await.seq;
    fixture.content_reads();
    assert_matches!(
        fixture.host.worker.steps(&fork_id, 0).build_step(one_file).await,
        Ok(GrepBuildOutcome::BackfillRestarted { captured_seq }) if captured_seq == fork_head
    );
    let restarted = entry(&fixture, &fork_id).await;
    let GrepIndexStatus::Backfilling {
        snapshot_id,
        cursor_inode_id: None,
        ..
    } = restarted.status()
    else {
        panic!("the fork restarts the backfill from the beginning");
    };
    assert!(
        control::snapshot_record(&fixture.store, &fork_id, snapshot_id)
            .await
            .is_some()
    );
    assert_eq!(restarted.runs(), source_entry.runs());
    assert!(
        control::snapshot_record(&fixture.store, &fixture.namespace_id, &source_snapshot_id)
            .await
            .is_some(),
        "the source keeps its own backfill snapshot"
    );

    fixture
        .host
        .catch_up_grep_index(&fork_id, fork_head)
        .await
        .expect("finish the fork's backfill");
    assert_eq!(
        fixture.content_reads(),
        2,
        "the walk reads the two values that the inherited segments don't record"
    );
    assert_eq!(
        paths(&grep(&fixture, &fork_id, "backfill needle").await).len(),
        3
    );
}

/// Checks that a fork's first step clears a reorganization that the fork
/// inherited, keeps every inherited segment, and that the fork's own
/// reorganization writes outputs that the fork owns.
#[tokio::test]
async fn a_forks_first_step_clears_an_inherited_reorganization() {
    let fixture = ContentFixture::new("inherit-reorganize").await;
    fixture.put("/a.txt", b"reorganized needle one\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture.put("/b.txt", b"reorganized needle two\n").await;
    fixture.build_step().await;
    let small_steps = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(1),
        max_decoded_input_rows_per_step: nonzero_usize(4),
        ..GramIndexBuildPolicy::default()
    };
    assert_matches!(
        fixture.reorganize_step(small_steps).await,
        GrepReorganizeOutcome::UnitWritten {
            completed: false,
            ..
        }
    );
    let source_entry = entry(&fixture, &fixture.namespace_id).await;
    assert!(source_entry.reorganize().is_some());

    let (fork_id, _fork_handle) = fork(&fixture).await;
    assert!(entry(&fixture, &fork_id).await.reorganize().is_some());
    assert_matches!(
        fixture
            .host
            .worker
            .steps(&fork_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await,
        Ok(GrepBuildOutcome::UpToDate { .. })
    );
    let cleared = entry(&fixture, &fork_id).await;
    assert!(cleared.reorganize().is_none());
    assert_eq!(cleared.runs(), source_entry.runs());

    for _ in 0..256 {
        if matches!(
            fixture
                .host
                .worker
                .steps(&fork_id, 0)
                .reorganize_step(small_steps)
                .await
                .expect("fork reorganize step"),
            GrepReorganizeOutcome::NotNeeded { .. }
        ) {
            break;
        }
    }
    let reorganized = entry(&fixture, &fork_id).await;
    assert!(reorganized.reorganize().is_none());
    assert!(reorganized
        .segments()
        .any(|segment| segment.owner_namespace_id == fork_id));
    assert_eq!(
        paths(&grep(&fixture, &fork_id, "reorganized needle").await),
        BTreeSet::from(["/a.txt".to_owned(), "/b.txt".to_owned()])
    );
    assert_eq!(
        entry(&fixture, &fixture.namespace_id).await,
        source_entry,
        "the source's reorganization is the source's own"
    );
}

/// Checks that a fork whose first change both clears an inherited
/// reorganization and restarts an inherited backfill, with an unfolded tail
/// of its own, publishes the cleared entry before the backfill's snapshot folds
/// the tail, so the restarted backfill and its steps publish.
#[tokio::test]
async fn a_forks_restart_publishes_after_clearing_an_inherited_reorganization() {
    let fixture = ContentFixture::new("inherit-both").await;
    fixture.put("/a.txt", b"both needle one\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture.put("/b.txt", b"both needle two\n").await;
    fixture.build_step().await;
    let small_steps = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(1),
        max_decoded_input_rows_per_step: nonzero_usize(4),
        ..GramIndexBuildPolicy::default()
    };
    assert_matches!(
        fixture.reorganize_step(small_steps).await,
        GrepReorganizeOutcome::UnitWritten {
            completed: false,
            ..
        }
    );
    // A rebootstrap of the source keeps its reorganization, so the fork
    // inherits a backfill and a reorganization together.
    fixture.put("/c.txt", b"both needle three\n").await;
    fixture.advance_retention_floor().await;
    assert_matches!(
        fixture.build_step().await,
        GrepBuildOutcome::BackfillRestarted { .. }
    );
    let source_entry = entry(&fixture, &fixture.namespace_id).await;
    assert_matches!(source_entry.status(), GrepIndexStatus::Backfilling { .. });
    assert!(source_entry.reorganize().is_some());

    let (fork_id, fork_handle) = fork(&fixture).await;
    fork_handle
        .put_file(
            "/own.txt",
            b"both needle in the fork\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write the fork");
    let mut steps = fixture.host.worker.steps(&fork_id, u64::MAX);
    assert_matches!(
        steps.build_step(GramIndexBuildPolicy::default()).await,
        Ok(GrepBuildOutcome::BackfillRestarted { .. })
    );
    loop {
        match steps
            .build_step(GramIndexBuildPolicy::default())
            .await
            .expect("fork build step")
        {
            GrepBuildOutcome::Indexed { .. } => {}
            GrepBuildOutcome::UpToDate { .. } => break,
            other => panic!("unexpected build outcome {other:?}"),
        }
    }
    assert_eq!(
        steps.publish().await.expect("publish"),
        GrepPublishOutcome::Published
    );
    let built = entry(&fixture, &fork_id).await;
    assert!(built.reorganize().is_none());
    assert_matches!(built.status(), GrepIndexStatus::Active { .. });
    let found = grep(&fixture, &fork_id, "both needle").await;
    assert_eq!(found.matches.len(), 4);
    assert_eq!(found.built_through_seq, found.head_seq);
}
