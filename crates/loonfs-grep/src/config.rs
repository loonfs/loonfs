//! Bounded step configuration for per-namespace grep drivers.

use crate::GramIndexBuildPolicy;
use serde::Deserialize;
use thiserror::Error;

/// Default cap on concurrently executing grep build or fold steps.
///
/// Mirrors `loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE`; both background
/// maintenance families default to two expensive namespace steps at once.
pub const DEFAULT_MAX_CONCURRENT_STEPS: usize = 2;

/// Bounded-work policy shared by embedded and standalone drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrepWorkerConfig {
    /// Build or fold steps allowed to execute concurrently across namespaces.
    pub max_concurrent_steps: usize,
    /// Revisions examined per build step.
    pub max_files_per_step: usize,
    /// Content bytes read per build step.
    pub max_content_bytes_per_step: u64,
    /// Rows per written grep segment.
    pub max_rows_per_segment: usize,
    /// Delta-level runs that trigger a fold.
    pub max_l0_runs: usize,
    /// Mid-level runs that trigger a base fold.
    pub max_mid_runs: usize,
    /// Rows merged by one fold step.
    pub max_fold_rows_per_step: usize,
}

impl GrepWorkerConfig {
    /// Returns the bounded build/fold policy represented by this config.
    pub fn build_policy(self) -> GramIndexBuildPolicy {
        GramIndexBuildPolicy {
            max_files_per_step: self.max_files_per_step,
            max_content_bytes_per_step: self.max_content_bytes_per_step,
            max_rows_per_segment: self.max_rows_per_segment,
            max_l0_runs: self.max_l0_runs,
            max_mid_runs: self.max_mid_runs,
            max_fold_rows_per_step: self.max_fold_rows_per_step,
        }
    }

    /// Rejects zero step budgets.
    pub fn validate(self) -> Result<(), GrepWorkerConfigError> {
        if self.max_concurrent_steps == 0 {
            return Err(GrepWorkerConfigError::InvalidField {
                field: "max_concurrent_steps",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if let Some(field) = self.build_policy().zero_budget_field() {
            return Err(GrepWorkerConfigError::InvalidField {
                field,
                reason: "must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for GrepWorkerConfig {
    fn default() -> Self {
        let policy = GramIndexBuildPolicy::default();
        Self {
            max_concurrent_steps: DEFAULT_MAX_CONCURRENT_STEPS,
            max_files_per_step: policy.max_files_per_step,
            max_content_bytes_per_step: policy.max_content_bytes_per_step,
            max_rows_per_segment: policy.max_rows_per_segment,
            max_l0_runs: policy.max_l0_runs,
            max_mid_runs: policy.max_mid_runs,
            max_fold_rows_per_step: policy.max_fold_rows_per_step,
        }
    }
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
    fn defaults_match_the_build_policy() {
        let config = GrepWorkerConfig::default();

        assert_eq!(config.build_policy(), GramIndexBuildPolicy::default());
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn zero_policy_fields_are_rejected() {
        for (field, config) in [
            (
                "max_concurrent_steps",
                GrepWorkerConfig {
                    max_concurrent_steps: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
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
            (
                "max_rows_per_segment",
                GrepWorkerConfig {
                    max_rows_per_segment: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
            (
                "max_l0_runs",
                GrepWorkerConfig {
                    max_l0_runs: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
            (
                "max_mid_runs",
                GrepWorkerConfig {
                    max_mid_runs: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
            (
                "max_fold_rows_per_step",
                GrepWorkerConfig {
                    max_fold_rows_per_step: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
        ] {
            assert_eq!(
                config.validate(),
                Err(GrepWorkerConfigError::InvalidField {
                    field,
                    reason: "must be greater than zero".to_owned(),
                })
            );
        }
    }
}
