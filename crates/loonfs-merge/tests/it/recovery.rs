//! Recovering a merge session whose state was lost, from the forks and the
//! merge index that the store records.

use crate::scripted_mechanics::{
    merges, recover, run_to_end, sandbox, Answer, ScriptedMechanics, Sent,
};
use loonfs_merge::{DefaultResolver, MergeFailure, MergeSessionStatus};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{CommitSeq, Merge, NextMergeRangeOutcome};

#[tokio::test]
async fn a_lost_state_recovers_the_new_fork_that_was_merging_and_merges_the_rest() {
    let other = namespace_id("other");
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(vec![other.clone(), sandbox(), rebased.clone()]),
        Answer::Namespace { rebased_from: None },
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork merged a piece, so the lost session was merging it.
        Answer::Range {
            next_after_seq: 35,
            source_head_seq: 40,
            merged: true,
        },
        Answer::Range {
            next_after_seq: 35,
            source_head_seq: 40,
            merged: true,
        },
        Answer::Committed(60),
        Answer::Deleted,
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    assert_eq!(session.state().new_fork_ids, std::slice::from_ref(&rebased));
    assert!(matches!(
        &session.state().status,
        MergeSessionStatus::Merging { fork_id, through_seq: CommitSeq(40), .. } if *fork_id == rebased
    ));
    let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;
    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(state.new_fork_ids.is_empty());
    let merges = merges(&mechanics.sent());
    assert_eq!(
        merges.len(),
        1,
        "the original fork never merges after its rebase"
    );
    assert_eq!(merges[0].0, rebased);
}

#[tokio::test]
async fn a_lost_state_rebases_again_when_the_new_fork_never_merged() {
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork never merged, and the lost state had the commit IDs
        // of its replay commits, so the session rebases again.
        Answer::range(10, 12),
        Answer::range(10, 20),
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    mechanics.assert_script_used();
    let state = session.state();
    let MergeSessionStatus::Rebasing {
        fork_id,
        through_seq,
        new_fork_id,
        reports,
        ..
    } = &state.status
    else {
        panic!(
            "the session rebases the original fork again: {:?}",
            state.status
        );
    };
    assert_eq!(*fork_id, sandbox());
    assert_eq!(*through_seq, CommitSeq(20));
    assert_ne!(*new_fork_id, rebased);
    assert!(reports.is_empty());
    assert_eq!(
        state.new_fork_ids,
        [rebased, new_fork_id.clone()],
        "both new forks are deleted at the end"
    );
    assert_eq!(
        mechanics.sent().last(),
        Some(&Sent::NextMergeRange { fork_id: sandbox() })
    );
}

#[tokio::test]
async fn a_lost_state_fails_when_a_new_forks_next_range_is_unknown() {
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The outcome doesn't say whether the new fork merged.
        Answer::RangeOutcome {
            source_head_seq: 12,
            outcome: NextMergeRangeOutcome::Unknown,
        },
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    mechanics.assert_script_used();
    assert_eq!(
        session.state().status,
        MergeSessionStatus::Failed {
            fork_id: rebased.clone(),
            failure: MergeFailure::UnknownRangeOutcome,
            reports: Vec::new(),
        }
    );
    assert_eq!(session.state().new_fork_ids, [rebased]);
}

/// Returns the next-range outcome of the original fork after the merge of
/// `new_fork` covered the fork's commits through `through_seq`.
fn covered(new_fork: &str, through_seq: u64) -> NextMergeRangeOutcome {
    NextMergeRangeOutcome::Covered {
        merge: Merge {
            source_namespace_id: sandbox(),
            source_after_seq: CommitSeq(5),
            source_through_seq: CommitSeq(through_seq),
        },
        covered_by_namespace_id: namespace_id(new_fork),
    }
}

/// Checks that a lost state after a `merged` session deleted its new forks
/// recovers as `merged` when the next-range query says that a new fork's
/// merge wrote the original fork's row through the fork's head, and sends no
/// merge.
#[tokio::test]
async fn a_lost_state_after_the_new_forks_were_deleted_recovers_as_merged() {
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(Vec::new()),
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-1", 20),
        },
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    mechanics.assert_script_used();
    assert_eq!(session.state().status, MergeSessionStatus::Merged);
    assert!(session.state().new_fork_ids.is_empty());
    let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;
    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(merges(&mechanics.sent()).is_empty());
}

/// Checks that a lost state recovers as `failed` when a new fork's merge
/// wrote the original fork's row through a sequence below the fork's head,
/// with a failure that identifies the new fork and the end of its row, and
/// sends no merge, because the fork's later commits can't merge.
#[tokio::test]
async fn a_lost_state_fails_when_the_fork_has_commits_after_a_new_forks_merge() {
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(Vec::new()),
        Answer::RangeOutcome {
            source_head_seq: 25,
            outcome: covered("rebase-1", 20),
        },
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    mechanics.assert_script_used();
    assert_eq!(
        session.state().status,
        MergeSessionStatus::Failed {
            fork_id: sandbox(),
            failure: MergeFailure::Covered {
                covered_by_namespace_id: namespace_id("rebase-1"),
                source_through_seq: CommitSeq(20),
            },
            reports: Vec::new(),
        }
    );
    let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;
    assert!(merges(&mechanics.sent()).is_empty(), "{:?}", state.status);
}

/// Checks that a lost state whose new fork never merged recovers as
/// `merged`, without another rebase, when the next-range query says that
/// another new fork's merge wrote the original fork's row through its head,
/// and that the session lists the new fork and deletes it.
#[tokio::test]
async fn a_lost_state_recovers_as_merged_when_another_merge_covered_the_rebased_fork() {
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::ForkSnapshots(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork never merged.
        Answer::range(10, 12),
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-2", 20),
        },
        Answer::Deleted,
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    assert_eq!(session.state().status, MergeSessionStatus::Merged);
    assert_eq!(session.state().new_fork_ids, std::slice::from_ref(&rebased));
    let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;
    mechanics.assert_script_used();
    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(state.new_fork_ids.is_empty());
    assert!(merges(&mechanics.sent()).is_empty());
    assert_eq!(
        mechanics.sent().last(),
        Some(&Sent::Delete {
            namespace_id: rebased
        })
    );
}
