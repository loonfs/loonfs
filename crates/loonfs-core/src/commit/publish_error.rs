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

/// Returns the result that stands after another attempt. A later failure does
/// not prove that an earlier put with an unknown outcome failed; only a
/// success or a receipt replay settles it.
pub fn reconcile_publish_attempt(
    previous: Option<Result<Commit, CoreError>>,
    current: Result<Commit, CoreError>,
) -> Result<Commit, CoreError> {
    match previous {
        Some(previous @ Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(_))))
            if current.is_err() =>
        {
            previous
        }
        _ => current,
    }
}
