//! Merge sessions against scripted server answers: the transitions of the
//! loop, and the requests that each transition sends.

use crate::scripted_mechanics::{
    conflict, covered, merges, recover, run_to_end, sandbox, Answer, RecordingResolver,
    ScriptedMechanics, Sent,
};
use loonfs_merge::{
    DefaultResolver, MergeError, MergeFailure, MergeSession, MergeSessionOutcome,
    MergeSessionStatus,
};
use loonfs_types::{CommitSeq, ErrorCode, NextMergeRangeOutcome};
use std::collections::BTreeSet;

#[tokio::test]
async fn a_fork_without_conflicts_merges_in_one_commit() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::Committed(31),
    ]);
    let session = recover(&mechanics, DefaultResolver).await;

    let (outcomes, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(state.new_fork_ids.is_empty());
    assert!(matches!(
        outcomes.as_slice(),
        [MergeSessionOutcome::MergeCommitted {
            committed_seq: CommitSeq(31),
            through_seq: CommitSeq(20),
            ..
        }]
    ));
    let merges = merges(&mechanics.sent());
    assert_eq!(merges.len(), 1);
    assert_eq!(merges[0].0, sandbox());
    assert_eq!(merges[0].1.through_seq, Some(CommitSeq(20)));
}

#[tokio::test]
async fn a_merge_too_large_merges_through_the_split_hint_and_then_the_rest() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::too_large(15),
        Answer::Committed(31),
        Answer::Committed(32),
    ]);
    let session = recover(&mechanics, DefaultResolver).await;

    let (outcomes, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(matches!(
        outcomes.as_slice(),
        [
            MergeSessionOutcome::Split {
                split_at_seq: CommitSeq(15),
                ..
            },
            MergeSessionOutcome::MergeCommitted {
                through_seq: CommitSeq(15),
                ..
            },
            MergeSessionOutcome::MergeCommitted {
                through_seq: CommitSeq(20),
                ..
            },
        ]
    ));
    let merges = merges(&mechanics.sent());
    let through_seqs: Vec<_> = merges
        .iter()
        .map(|(_, options)| options.through_seq)
        .collect();
    assert_eq!(
        through_seqs,
        [
            Some(CommitSeq(20)),
            Some(CommitSeq(15)),
            Some(CommitSeq(20))
        ]
    );
    let commit_ids: BTreeSet<_> = merges
        .iter()
        .map(|(_, options)| &options.commit_id)
        .collect();
    assert_eq!(commit_ids.len(), 3, "each piece is a merge of its own");
}

#[tokio::test]
async fn a_conflict_rebases_in_pieces_and_the_resolved_new_fork_merges() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::refused(ErrorCode::MergeConflict),
        Answer::Forked,
        Answer::too_large(15),
        // The new fork exists for every later replay commit.
        Answer::refused(ErrorCode::NamespaceExists),
        Answer::Replayed(vec![conflict("/a.txt")]),
        Answer::refused(ErrorCode::NamespaceExists),
        Answer::Replayed(vec![conflict("/b.txt")]),
        Answer::range(40, 43),
        Answer::Committed(50),
        // The new fork's merge wrote the original fork's row through 20.
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-1", 20),
        },
        Answer::Deleted,
    ]);
    let resolver = RecordingResolver::default();
    let session = recover(&mechanics, resolver.clone()).await;

    let (_, state) = run_to_end(&mechanics, &resolver, session).await;

    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(
        state.new_fork_ids.is_empty(),
        "the session deletes its new fork"
    );
    let sent = mechanics.sent();
    let rebases: Vec<_> = sent
        .iter()
        .filter_map(|sent| match sent {
            Sent::Replay {
                fork_id,
                new_fork_id,
                options,
            } => Some((fork_id, new_fork_id, options)),
            _ => None,
        })
        .collect();
    let new_fork_id = rebases[0].1.clone();
    assert!(rebases
        .iter()
        .all(|(fork_id, into, _)| **fork_id == sandbox() && **into == new_fork_id));
    let through_seqs: Vec<_> = rebases
        .iter()
        .map(|(_, _, options)| options.through_seq)
        .collect();
    assert_eq!(
        through_seqs,
        [
            Some(CommitSeq(20)),
            Some(CommitSeq(15)),
            Some(CommitSeq(20))
        ],
        "the rebase replays the whole unmerged range, in pieces"
    );
    let resolved = resolver.resolved();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].0, new_fork_id);
    let report_ends: Vec<_> = resolved[0]
        .1
        .iter()
        .map(|report| report.through_seq)
        .collect();
    assert_eq!(
        report_ends,
        [CommitSeq(15), CommitSeq(20)],
        "the resolver gets every piece's report"
    );
    let merges = merges(&sent);
    assert_eq!(
        merges.last().map(|(fork_id, _)| fork_id),
        Some(&new_fork_id)
    );
    assert_eq!(
        merges.last().and_then(|(_, options)| options.through_seq),
        Some(CommitSeq(43))
    );
    assert_eq!(
        sent[sent.len() - 2..],
        [
            Sent::NextMergeRange { fork_id: sandbox() },
            Sent::Delete {
                namespace_id: new_fork_id
            }
        ],
        "the session reads the original fork's row before it deletes the new fork"
    );
}

#[tokio::test]
async fn the_default_resolver_merges_a_new_fork_whose_rebase_left_out_nothing() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::refused(ErrorCode::MergeConflict),
        Answer::Forked,
        Answer::too_large(15),
        Answer::refused(ErrorCode::NamespaceExists),
        Answer::Replayed(Vec::new()),
        Answer::refused(ErrorCode::NamespaceExists),
        Answer::Replayed(Vec::new()),
        Answer::range(40, 43),
        Answer::Committed(50),
        // The new fork's merge wrote the original fork's row through 20.
        Answer::RangeOutcome {
            source_head_seq: 20,
            outcome: covered("rebase-1", 20),
        },
        Answer::Deleted,
    ]);
    let session = recover(&mechanics, DefaultResolver).await;

    let (outcomes, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

    assert_eq!(state.status, MergeSessionStatus::Merged, "{outcomes:?}");
    assert!(state.new_fork_ids.is_empty());
    let merges = merges(&mechanics.sent());
    let (new_fork_id, _) = merges.last().expect("the new fork merges");
    assert_ne!(*new_fork_id, sandbox());
    assert!(outcomes.contains(&MergeSessionOutcome::Resolved {
        fork_id: new_fork_id.clone(),
        through_seq: CommitSeq(43),
    }));
}

#[tokio::test]
async fn a_new_fork_whose_range_starts_below_its_floor_fails_with_the_reports() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::refused(ErrorCode::MergeConflict),
        Answer::Forked,
        Answer::Replayed(vec![conflict("/a.txt")]),
        Answer::RangeOutcome {
            source_head_seq: 43,
            outcome: NextMergeRangeOutcome::RangeStartBelowFloor {
                merge: None,
                next_after_seq: CommitSeq(40),
                retention_floor_seq: CommitSeq(42),
            },
        },
    ]);
    let resolver = RecordingResolver::default();
    let session = recover(&mechanics, resolver.clone()).await;

    let (outcomes, state) = run_to_end(&mechanics, &resolver, session).await;

    assert_eq!(outcomes.last(), Some(&MergeSessionOutcome::Failed));
    let MergeSessionStatus::Failed {
        fork_id,
        failure,
        reports,
    } = &state.status
    else {
        panic!("the session should fail: {:?}", state.status);
    };
    assert_eq!(
        *failure,
        MergeFailure::RangeStartBelowFloor {
            retention_floor_seq: CommitSeq(42)
        }
    );
    assert_eq!(
        state.new_fork_ids,
        std::slice::from_ref(fork_id),
        "the failure identifies the new fork to inspect"
    );
    let resolved = resolver.resolved();
    assert_eq!(resolved.len(), 1);
    assert_eq!(*reports, resolved[0].1, "the failure keeps the reports");
    assert_eq!(
        merges(&mechanics.sent()).len(),
        1,
        "the session doesn't merge the new fork"
    );
}

#[tokio::test]
async fn a_fork_point_below_the_floor_fails_the_session_before_it_merges() {
    let cases = [
        (
            NextMergeRangeOutcome::ForkPointBelowFloor {
                retention_floor_seq: CommitSeq(15),
            },
            MergeFailure::ForkPointBelowFloor {
                retention_floor_seq: CommitSeq(15),
            },
        ),
        (
            NextMergeRangeOutcome::Unknown,
            MergeFailure::UnknownRangeOutcome,
        ),
    ];
    for (outcome, failure) in cases {
        let mechanics = ScriptedMechanics::new([
            Answer::RangeOutcome {
                source_head_seq: 20,
                outcome,
            },
            Answer::ForkSnapshots(Vec::new()),
        ]);
        let session = recover(&mechanics, DefaultResolver).await;

        let (outcomes, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

        assert!(outcomes.is_empty(), "{outcomes:?}");
        assert_eq!(
            state.status,
            MergeSessionStatus::Failed {
                fork_id: sandbox(),
                failure,
                reports: Vec::new(),
            }
        );
    }
}

#[tokio::test]
async fn a_busy_server_leaves_the_state_as_it_was_and_the_next_step_sends_the_same_merge() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::refused(ErrorCode::ServerBusy),
        Answer::Committed(31),
    ]);
    let mut session = recover(&mechanics, DefaultResolver).await;
    let before = session.state().clone();

    let busy = session.step().await;

    assert!(
        matches!(&busy, Err(MergeError::Client(error)) if error.code() == Some(ErrorCode::ServerBusy)),
        "{busy:?}"
    );
    assert_eq!(*session.state(), before);
    let (outcomes, state) = run_to_end(&mechanics, &DefaultResolver, session).await;
    assert_eq!(state.status, MergeSessionStatus::Merged);
    assert!(matches!(
        outcomes.as_slice(),
        [MergeSessionOutcome::MergeCommitted {
            committed_seq: CommitSeq(31),
            ..
        }]
    ));
    let merges = merges(&mechanics.sent());
    assert_eq!(merges.len(), 2);
    assert_eq!(
        merges[0], merges[1],
        "the next step sends the same request and its commit ID"
    );
}

#[tokio::test]
async fn errors_that_the_loop_cant_act_on_fail_the_session() {
    let cases: [(&str, Vec<Answer>, ErrorCode); 3] = [
        (
            "a namespace that changed after each of the server's plans",
            vec![Answer::refused(ErrorCode::StaleHead)],
            ErrorCode::StaleHead,
        ),
        (
            "a first unmerged commit that's too large alone",
            vec![Answer::refused(ErrorCode::MergeTooLarge)],
            ErrorCode::MergeTooLarge,
        ),
        (
            "a fork point below the retention floor",
            vec![Answer::refused(ErrorCode::BelowRetentionFloor)],
            ErrorCode::BelowRetentionFloor,
        ),
    ];
    for (case, answers, code) in cases {
        let mechanics = ScriptedMechanics::new(
            [Answer::range(10, 20), Answer::ForkSnapshots(Vec::new())]
                .into_iter()
                .chain(answers),
        );
        let session = recover(&mechanics, DefaultResolver).await;

        let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

        let MergeSessionStatus::Failed {
            fork_id,
            failure: MergeFailure::Refused { error },
            ..
        } = &state.status
        else {
            panic!("{case}: the session should fail: {:?}", state.status);
        };
        assert_eq!(*fork_id, sandbox(), "{case}");
        assert_eq!(error.code, code.as_str(), "{case}");
        let mut session = MergeSession::resume(mechanics.clone(), DefaultResolver, state);
        session
            .abandon()
            .expect("a failed session can be abandoned");
        assert!(
            matches!(
                session.abandon(),
                Err(MergeError::NotFailed {
                    status: "abandoned"
                })
            ),
            "{case}"
        );
    }
}

#[tokio::test]
async fn a_rebase_whose_new_fork_cant_be_created_fails_without_a_replay() {
    let mechanics = ScriptedMechanics::new([
        Answer::range(10, 20),
        Answer::ForkSnapshots(Vec::new()),
        Answer::refused(ErrorCode::MergeConflict),
        Answer::refused(ErrorCode::BelowRetentionFloor),
    ]);
    let session = recover(&mechanics, DefaultResolver).await;

    let (_, state) = run_to_end(&mechanics, &DefaultResolver, session).await;

    let MergeSessionStatus::Failed {
        fork_id,
        failure: MergeFailure::Refused { error },
        ..
    } = &state.status
    else {
        panic!("the session should fail: {:?}", state.status);
    };
    assert_eq!(*fork_id, sandbox());
    assert_eq!(error.code, ErrorCode::BelowRetentionFloor.as_str());
    assert!(
        !mechanics
            .sent()
            .iter()
            .any(|sent| matches!(sent, Sent::Replay { .. })),
        "the session sends no replay commit into a new fork that it couldn't create"
    );
}
