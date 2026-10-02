//! Grep worker step budgets, and how many steps hold file content at once.

use crate::{GramIndexBuildPolicy, DEFAULT_MAX_CONCURRENT_GREP_STEPS};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};
use thiserror::Error;

/// Operator-facing work budgets shared by hosts that run grep steps.
/// Segment layout and merge policy remain engine defaults.
///
/// Project-wide, zero may disable an explicitly documented cache. Work
/// budgets instead reject zero at their construction boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrepWorkerConfig {
    /// Revisions examined per build step.
    pub max_files_per_step: usize,
    /// Content bytes read per build step.
    pub max_content_bytes_per_step: u64,
    /// Build and reorganize steps that hold file content or index segments
    /// at once, across every namespace one worker indexes.
    pub max_concurrent_steps: usize,
}

impl GrepWorkerConfig {
    /// Returns the bounded build/reorganize policy represented by this config.
    pub fn build_policy(self) -> Result<GramIndexBuildPolicy, GrepWorkerConfigError> {
        Ok(GramIndexBuildPolicy {
            max_files_per_step: nonzero_usize("max_files_per_step", self.max_files_per_step)?,
            max_content_bytes_per_step: nonzero_u64(
                "max_content_bytes_per_step",
                self.max_content_bytes_per_step,
            )?,
            ..GramIndexBuildPolicy::default()
        })
    }

    /// Rejects zero step budgets and a zero step limit.
    pub fn validate(self) -> Result<(), GrepWorkerConfigError> {
        self.build_policy()?;
        nonzero_usize("max_concurrent_steps", self.max_concurrent_steps)?;
        Ok(())
    }
}

impl Default for GrepWorkerConfig {
    fn default() -> Self {
        let policy = GramIndexBuildPolicy::default();
        Self {
            max_files_per_step: policy.max_files_per_step.get(),
            max_content_bytes_per_step: policy.max_content_bytes_per_step.get(),
            max_concurrent_steps: DEFAULT_MAX_CONCURRENT_GREP_STEPS.get(),
        }
    }
}

fn nonzero_usize(field: &'static str, value: usize) -> Result<NonZeroUsize, GrepWorkerConfigError> {
    NonZeroUsize::new(value).ok_or_else(|| GrepWorkerConfigError::InvalidField {
        field,
        reason: "must be greater than zero".to_owned(),
    })
}

fn nonzero_u64(field: &'static str, value: u64) -> Result<NonZeroU64, GrepWorkerConfigError> {
    NonZeroU64::new(value).ok_or_else(|| GrepWorkerConfigError::InvalidField {
        field,
        reason: "must be greater than zero".to_owned(),
    })
}

/// Invalid grep worker configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepWorkerConfigError {
    /// One field cannot safely drive the worker.
    #[error("invalid `{field}`: {reason}")]
    InvalidField {
        /// Field within the `[grep]` table.
        field: &'static str,
        /// Human-readable rejection reason.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_construction_rejects_each_zero_budget() {
        for (field, config) in [
            (
                "max_files_per_step",
                GrepWorkerConfig {
                    max_files_per_step: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
            (
                "max_content_bytes_per_step",
                GrepWorkerConfig {
                    max_content_bytes_per_step: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
        ] {
            assert_eq!(
                config.build_policy(),
                Err(GrepWorkerConfigError::InvalidField {
                    field,
                    reason: "must be greater than zero".to_owned(),
                })
            );
        }
    }
}
