//! Shared pacing and step-budget configuration for grep worker hosts.

use crate::GramIndexBuildPolicy;
use serde::Deserialize;
use thiserror::Error;

/// Default delay between all-namespace build/fold sweeps.
pub const DEFAULT_GREP_STEP_INTERVAL_MS: u64 = 1_000;
/// Default delay between grep-owned garbage-collection passes.
pub const DEFAULT_GREP_GC_INTERVAL_MS: u64 = 60_000;

/// Pacing and bounded-work policy shared by embedded and standalone workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrepWorkerConfig {
    /// Delay between all-namespace build/fold sweeps.
    pub step_interval_ms: u64,
    /// Delay between grep-owned garbage-collection passes.
    pub gc_interval_ms: u64,
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

    /// Rejects zero pacing intervals and zero step budgets.
    pub fn validate(self) -> Result<(), GrepWorkerConfigError> {
        if self.step_interval_ms == 0 {
            return Err(GrepWorkerConfigError::InvalidField {
                field: "step_interval_ms",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.gc_interval_ms == 0 {
            return Err(GrepWorkerConfigError::InvalidField {
                field: "gc_interval_ms",
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
            step_interval_ms: DEFAULT_GREP_STEP_INTERVAL_MS,
            gc_interval_ms: DEFAULT_GREP_GC_INTERVAL_MS,
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
    fn defaults_match_the_build_policy_and_worker_pacing() {
        let config = GrepWorkerConfig::default();

        assert_eq!(config.step_interval_ms, 1_000);
        assert_eq!(config.gc_interval_ms, 60_000);
        assert_eq!(config.build_policy(), GramIndexBuildPolicy::default());
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn zero_pacing_and_policy_fields_are_rejected() {
        for (field, config) in [
            (
                "step_interval_ms",
                GrepWorkerConfig {
                    step_interval_ms: 0,
                    ..GrepWorkerConfig::default()
                },
            ),
            (
                "gc_interval_ms",
                GrepWorkerConfig {
                    gc_interval_ms: 0,
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
