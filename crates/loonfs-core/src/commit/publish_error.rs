//! Failures while publishing a numbered WAL object, and how a retry treats them.

use crate::error::CoreError;
use loonfs_api::v0::Commit;
use loonfs_api::ErrorCode;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WalPublishError {
    #[error("WAL number was taken by another publication")]
    StaleHead,
    #[error("publish budget exceeded: elapsed {elapsed_ms}ms over budget {budget_ms}ms")]
    PublishBudgetExceeded { elapsed_ms: u64, budget_ms: u64 },
    #[error("WAL publication outcome unknown: {0}")]
    OutcomeUnknown(String),
}

impl WalPublishError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::StaleHead | Self::PublishBudgetExceeded { .. } => ErrorCode::StaleHead,
            Self::OutcomeUnknown(_) => ErrorCode::CommitOutcomeUnknown,
        }
    }
}

/// Returns whether a candidate with this result is published again: a taken
/// WAL number, an expired budget, or an unknown outcome. The retry keeps the
/// commit ID, so a candidate that already committed replays its receipt.
pub fn is_retryable_wal_publish(result: &Result<Commit, CoreError>) -> bool {
    matches!(
        result,
        Err(CoreError::WalPublish(
            WalPublishError::StaleHead
                | WalPublishError::PublishBudgetExceeded { .. }
                | WalPublishError::OutcomeUnknown(_)
        ))
    )
}

/// Records one attempt's results and returns the candidates to publish again.
///
/// A candidate is published again only while its own result is retried. A
/// settled result is not attempted again, so a later failure cannot replace
/// it. A later failure also does not prove that an earlier put with an unknown
/// outcome failed; only a success or a receipt replay settles that.
pub fn settle_publish_attempt<C>(
    results: &mut [Option<Result<Commit, CoreError>>],
    attempted: impl IntoIterator<Item = (usize, C)>,
    observed: Vec<Result<Commit, CoreError>>,
) -> Vec<(usize, C)> {
    let mut pending = Vec::new();
    for ((index, candidate), current) in attempted.into_iter().zip(observed) {
        if is_retryable_wal_publish(&current) {
            pending.push((index, candidate));
        }
        let unknown = matches!(
            &results[index],
            Some(Err(CoreError::WalPublish(error))) if matches!(error, WalPublishError::OutcomeUnknown(_))
        );
        if !(unknown && current.is_err()) {
            results[index] = Some(current);
        }
    }
    pending
}
