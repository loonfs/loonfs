//! Failures while publishing a numbered WAL object.

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
