//! Recovering a merge session whose state was lost, from the forks and the
//! merge index that the store records.

use crate::scripted_mechanics::{
    covered, main, merges, recover, run_to_end, sandbox, Answer, ScriptedMechanics, Sent,
};
use loonfs_merge::{
    DefaultResolver, MergeFailure, MergeSession, MergeSessionOptions, MergeSessionStatus,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{CommitSeq, NextMergeRangeOutcome};

#[tokio::test]
async fn a_lost_state_recovers_the_new_fork_that_was_merging_and_merges_the_rest() {
    let other = namespace_id("other");
    let rebased = namespace_id("rebase-1");
    let original_row = || Answer::RangeOutcome {
        source_head_seq: 20,
        outcome: covered("rebase-1", 20),
    };
    let mechanics = ScriptedMechanics::new([
        original_row(),
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
        original_row(),
        Answer::Deleted,
    ]);

    let session = recover(&mechanics, DefaultResolver).await;

    assert_eq!(session.state().through_seq, CommitSeq(20));
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
        Answer::range(10, 20),
        Answer::ForkSnapshots(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork never merged, and the lost state had the commit IDs
        // of its replay commits, so the session rebases again.
        Answer::range(10, 12),
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
        mechanics.sent().first(),
        Some(&Sent::NextMergeRange { fork_id: sandbox() })
    );
}

#[tokio::test]
async fn a_lost_state_fails_when_a_new_forks_next_range_is_unknown() {
    let rebased = namespace_id("rebase-1");
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
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

/// Checks that a lost state after a `merged` session deleted its new forks
/// recovers as `merged` when the next-range query says that a new fork's
/// merge wrote the original fork's row through the fork's head, and sends no
/// merge.
#[tokio::test]
async fn a_lost_state_after_the_new_forks_were_deleted_recovers_as_merged() {
    let mechanics = ScriptedMechanics::new([
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-1", 20),
        },
        Answer::ForkSnapshots(Vec::new()),
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
        Answer::RangeOutcome {
            source_head_seq: 25,
            outcome: covered("rebase-1", 20),
        },
        Answer::ForkSnapshots(Vec::new()),
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
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-2", 20),
        },
        Answer::ForkSnapshots(vec![rebased.clone()]),
        Answer::Namespace {
            rebased_from: Some(sandbox()),
        },
        // The new fork never merged.
        Answer::range(10, 12),
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

/// Checks that a lost state whose new fork merged and still exists reads the
/// original fork's row before it reports `merged`. The new fork's merge
/// wrote the original fork's row through 20 and the original fork's head is
/// 25. Asked to merge through 25, the session is `failed` with the new fork
/// and 20, keeps the new fork, and sends no merge. Asked to merge through
/// 20, the session is `merged` and deletes the new fork.
#[tokio::test]
async fn a_lost_state_whose_new_fork_merged_checks_the_original_forks_row() {
    let rebased = namespace_id("rebase-1");
    for (through_seq, merged) in [(25, false), (20, true)] {
        let original_row = || Answer::RangeOutcome {
            source_head_seq: 25,
            outcome: covered("rebase-1", 20),
        };
        let mut script = vec![
            original_row(),
            Answer::ForkSnapshots(vec![rebased.clone()]),
            Answer::Namespace {
                rebased_from: Some(sandbox()),
            },
            // The new fork merged through its head.
            Answer::Range {
                next_after_seq: 35,
                source_head_seq: 35,
                merged: true,
            },
            Answer::Range {
                next_after_seq: 35,
                source_head_seq: 35,
                merged: true,
            },
            original_row(),
        ];
        if merged {
            script.push(Answer::Deleted);
        }
        let mechanics = ScriptedMechanics::new(script);
        let options = MergeSessionOptions {
            through_seq: Some(CommitSeq(through_seq)),
        };
        let session = MergeSession::recover(
            mechanics.clone(),
            DefaultResolver,
            main(),
            sandbox(),
            loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("recover the session");

        let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

        mechanics.assert_script_used();
        assert!(merges(&mechanics.sent()).is_empty(), "{through_seq}");
        if merged {
            assert_eq!(state.status, MergeSessionStatus::Merged);
            assert!(state.new_fork_ids.is_empty());
        } else {
            assert_eq!(
                state.status,
                MergeSessionStatus::Failed {
                    fork_id: sandbox(),
                    failure: MergeFailure::Covered {
                        covered_by_namespace_id: rebased.clone(),
                        source_through_seq: CommitSeq(20),
                    },
                    reports: Vec::new(),
                }
            );
            assert_eq!(state.new_fork_ids, std::slice::from_ref(&rebased));
            assert!(
                !mechanics
                    .sent()
                    .iter()
                    .any(|sent| matches!(sent, Sent::Delete { .. })),
                "a failed session keeps its new fork"
            );
        }
    }
}

/// Checks that a lost state whose new fork has a piece left to merge reads
/// the original fork's row after the session merges that piece. The new fork
/// replayed the original fork through 20, and its last piece (through its
/// head, 40) contains only the resolver's changes, so merging it leaves the
/// original fork's row at 20. Asked to merge through 25, or through the
/// original fork's head, 25, the session is `failed` with the new fork and
/// 20, and keeps the new fork. Asked to merge through 20, the session is
/// `merged` and deletes the new fork.
#[tokio::test]
async fn a_lost_state_whose_new_fork_has_a_piece_left_checks_the_original_forks_row() {
    let rebased = namespace_id("rebase-1");
    for (through_seq, merged) in [(Some(25), false), (None, false), (Some(20), true)] {
        let original_row = || Answer::RangeOutcome {
            source_head_seq: 25,
            outcome: covered("rebase-1", 20),
        };
        let new_fork_range = || Answer::Range {
            next_after_seq: 35,
            source_head_seq: 40,
            merged: true,
        };
        let mut script = vec![
            original_row(),
            Answer::ForkSnapshots(vec![rebased.clone()]),
            Answer::Namespace {
                rebased_from: Some(sandbox()),
            },
            new_fork_range(),
            new_fork_range(),
            Answer::Committed(60),
            original_row(),
        ];
        if merged {
            script.push(Answer::Deleted);
        }
        let mechanics = ScriptedMechanics::new(script);
        let options = MergeSessionOptions {
            through_seq: through_seq.map(CommitSeq),
        };
        let session = MergeSession::recover(
            mechanics.clone(),
            DefaultResolver,
            main(),
            sandbox(),
            loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("recover the session");
        let case = format!("{through_seq:?}");
        assert!(
            matches!(
                &session.state().status,
                MergeSessionStatus::Merging { fork_id, through_seq: CommitSeq(40), .. }
                    if *fork_id == rebased
            ),
            "{case}"
        );

        let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

        let merges = merges(&mechanics.sent());
        assert_eq!(merges.len(), 1, "{case}");
        assert_eq!(merges[0].0, rebased, "{case}");
        if merged {
            assert_eq!(state.status, MergeSessionStatus::Merged, "{case}");
            assert!(state.new_fork_ids.is_empty(), "{case}");
        } else {
            assert_eq!(
                state.status,
                MergeSessionStatus::Failed {
                    fork_id: sandbox(),
                    failure: MergeFailure::Covered {
                        covered_by_namespace_id: rebased.clone(),
                        source_through_seq: CommitSeq(20),
                    },
                    reports: Vec::new(),
                },
                "{case}"
            );
            assert_eq!(state.new_fork_ids, std::slice::from_ref(&rebased), "{case}");
            assert!(
                !mechanics
                    .sent()
                    .iter()
                    .any(|sent| matches!(sent, Sent::Delete { .. })),
                "{case}: a failed session keeps its new fork"
            );
        }
    }
}
