//! Prepares each commit with the live write path and with the frozen copy,
//! and requires the two results to agree.

use super::write::PublishPlanningSession;
use crate::authorize::Authorizer;
use crate::commit::engine::CommitCandidate;
use crate::commit::{CommitFingerprint, CommitPlan};
use crate::error::Result;
use loonfs_objectstore::ObjectStore;
use loonfs_types::write_path_comparison::{record_draws, replay_draws, ReplayOutcome};
use std::future::Future;

/// Prepares `candidate` with `live` and then with the frozen copy in `frozen`,
/// compares the two results, and returns the live result.
///
/// The live preparation runs first and draws fresh random bytes, and the
/// frozen copy is given the same draws in the same order, so the IDs that the
/// two preparations generate are equal. The frozen copy repeats the live
/// preparation's store requests. Its write of a span object goes to the key
/// that the live preparation wrote, with the same bytes, which the store
/// accepts as a retry of the same write.
pub(crate) fn prepare_and_compare<'a, S, L>(
    live: L,
    frozen: &'a PublishPlanningSession<'a, S>,
    candidate: &'a CommitCandidate,
    authorizer: Authorizer<'a>,
    semantic_identity: CommitFingerprint,
) -> impl Future<Output = Result<CommitPlan>> + 'a
where
    S: ObjectStore + ?Sized,
    L: Future<Output = Result<CommitPlan>> + 'a,
{
    // Both preparations are boxed, so the layout of this future doesn't
    // contain theirs, which would exceed the compiler's query depth limit.
    let live = record_draws(live);
    async move {
        let (live_result, draws) = live.await;
        let (frozen_result, replay) = replay_draws(
            frozen.prepare_commit(candidate, authorizer, semantic_identity),
            draws,
        )
        .await;
        compare_prepared_commits(&live_result, &frozen_result, replay);
        live_result
    }
}

/// Panics when the live and frozen preparations of one commit disagree.
///
/// Two plans must be equal. Two errors must have the same code, the same
/// details (which include the failing operation's or precondition's index),
/// the same message text, and the same structure, which includes the field
/// that the HTTP layer reports as `param`. Both preparations must use exactly
/// the same random draws. A preparation that fails in the object store makes
/// the comparison inconclusive, because a fault can hit one preparation and
/// not the other.
#[expect(
    clippy::print_stderr,
    reason = "a mismatch prints both results in full before the assertion fails"
)]
fn compare_prepared_commits(
    live: &Result<CommitPlan>,
    frozen: &Result<CommitPlan>,
    replay: ReplayOutcome,
) {
    let store_failure = |result: &Result<CommitPlan>| {
        result
            .as_ref()
            .is_err_and(|error| error.object_store_public_message().is_some())
    };
    if store_failure(live) || store_failure(frozen) {
        tracing::debug!(
            "the write path comparison is inconclusive because a preparation failed in the object store"
        );
        return;
    }
    let mismatch = if replay.missing_draws > 0 || replay.leftover_draws > 0 {
        Some(format!(
            "the frozen copy asked for {} random draws that the live code didn't make and left {} \
             unused",
            replay.missing_draws, replay.leftover_draws
        ))
    } else {
        match (live, frozen) {
            (Ok(live), Ok(frozen)) => (live != frozen).then(|| "the plans differ".to_owned()),
            (Ok(_), Err(_)) => {
                Some("the live code planned the commit and the frozen copy failed".to_owned())
            }
            (Err(_), Ok(_)) => {
                Some("the live code failed and the frozen copy planned the commit".to_owned())
            }
            (Err(live), Err(frozen)) => {
                if live.code() != frozen.code() {
                    Some("the error codes differ".to_owned())
                } else if live.details() != frozen.details() {
                    Some("the error details differ".to_owned())
                } else if live.to_string() != frozen.to_string() {
                    Some("the error messages differ".to_owned())
                } else if format!("{live:?}") != format!("{frozen:?}") {
                    Some("the errors differ".to_owned())
                } else {
                    None
                }
            }
        }
    };
    if let Some(mismatch) = &mismatch {
        eprintln!(
            "write path comparison mismatch: {mismatch}\nlive result: {live:#?}\nfrozen result: \
             {frozen:#?}"
        );
    }
    assert!(
        mismatch.is_none(),
        "write path comparison mismatch: {}",
        mismatch.unwrap_or_default()
    );
}

#[cfg(test)]
mod tests {
    use super::compare_prepared_commits;
    use crate::commit::{CommitPlan, CommitValidationError};
    use crate::error::{CoreError, Result};
    use loonfs_types::write_path_comparison::ReplayOutcome;
    use loonfs_types::{CommitId, CommitSeq, InodeId, NamespaceId, WriterEpoch};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    const EXACT: ReplayOutcome = ReplayOutcome {
        missing_draws: 0,
        leftover_draws: 0,
    };

    fn plan(assigned_seq: u64) -> CommitPlan {
        CommitPlan {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: CommitId::parse("commit").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            message: None,
            merge: None,
            semantic_identity: loonfs_test_support::ids::commit_fingerprint("test"),
            apply_after_seq: CommitSeq(assigned_seq - 1),
            assigned_seq: CommitSeq(assigned_seq),
            committed_at_ms: 1,
            deltas: Vec::new(),
            appended: Vec::new(),
            inline_content: Vec::new(),
        }
    }

    fn invalid_request(message: &str) -> CoreError {
        CoreError::InvalidCommitRequest(message.to_owned()).at_operation(0)
    }

    fn mismatch_message(
        live: &Result<CommitPlan>,
        frozen: &Result<CommitPlan>,
        replay: ReplayOutcome,
    ) -> String {
        let panic = catch_unwind(AssertUnwindSafe(|| {
            compare_prepared_commits(live, frozen, replay);
        }))
        .expect_err("the comparison should report a difference");
        panic
            .downcast_ref::<String>()
            .cloned()
            .expect("the panic message should be a formatted string")
    }

    #[test]
    fn the_comparison_reports_every_kind_of_difference() {
        let not_empty = CoreError::from(CommitValidationError::DirectoryNotEmpty {
            inode_id: InodeId(1),
        })
        .at_operation(0);
        let cases: [(Result<CommitPlan>, Result<CommitPlan>, ReplayOutcome); 7] = [
            (Ok(plan(2)), Ok(plan(3)), EXACT),
            (Ok(plan(2)), Err(invalid_request("a")), EXACT),
            (Err(invalid_request("a")), Ok(plan(2)), EXACT),
            (Err(invalid_request("a")), Err(not_empty), EXACT),
            (Err(invalid_request("a")), Err(invalid_request("b")), EXACT),
            (
                Ok(plan(2)),
                Ok(plan(2)),
                ReplayOutcome {
                    missing_draws: 1,
                    leftover_draws: 0,
                },
            ),
            (
                Ok(plan(2)),
                Ok(plan(2)),
                ReplayOutcome {
                    missing_draws: 0,
                    leftover_draws: 1,
                },
            ),
        ];
        for (live, frozen, replay) in &cases {
            let message = mismatch_message(live, frozen, *replay);
            assert!(
                message.starts_with("write path comparison mismatch"),
                "unexpected panic message: {message}"
            );
        }
    }

    #[test]
    fn the_comparison_accepts_equal_results() {
        compare_prepared_commits(&Ok(plan(2)), &Ok(plan(2)), EXACT);
        compare_prepared_commits(
            &Err(invalid_request("a")),
            &Err(invalid_request("a")),
            EXACT,
        );
    }
}
