//! Recovering a merge session whose state was lost, from the forks and the
//! merge index that the store records.

use crate::scripted_mechanics::{
    merges, recover, run_to_end, sandbox, Answer, ScriptedMechanics, Sent,
};
use loonfs_merge::{DefaultResolver, MergeFailure, MergeSessionStatus};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{CommitSeq, NextMergeRangeOutcome};

#[tokio::test]
async fn a_lost_state_recovers_the_new_fork_that_was_merging_and_merges_the_rest() {
    let other = namespace_id("other");
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::ForkPins(vec![other.clone(), sandbox(), rebased.clone()]),
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
        Answer::ForkPins(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork never merged, so its conflict reports are lost.
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
        Answer::ForkPins(vec![rebased.clone()]),
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
