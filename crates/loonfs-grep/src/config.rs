//! Grep worker step budgets.
//!
//! How many steps run at once is not here and never will be: every host
//! that schedules grep does it through the runtime's maintenance runner,
//! whose one permit pool (`max_concurrent_maintenance`) bounds every
//! maintenance family together.

use crate::GramIndexBuildPolicy;
use serde::Deserialize;
use std::num::{NonZeroU64, NonZeroUsize};
use thiserror::Error;

/// Bounded-work policy shared by every host that runs grep steps.
///
/// Project-wide, zero may disable an explicitly documented cache. Work
/// budgets instead reject zero at their construction boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrepWorkerConfig {
    /// Revisions examined per build step.
    pub max_files_per_step: usize,
    /// Content bytes read per build step.
    pub max_content_bytes_per_step: u64,
    /// Rows per written grep segment.
    pub max_rows_per_segment: usize,
    /// Delta-level runs that trigger a reorganization into a mid run.
    pub max_l0_runs: usize,
    /// Mid-level runs that trigger a reorganization into the base run.
    pub max_mid_runs: usize,
    /// Rows merged by one reorganize step.
    pub max_decoded_input_rows_per_step: usize,
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
            max_rows_per_segment: nonzero_usize("max_rows_per_segment", self.max_rows_per_segment)?,
            max_l0_runs: nonzero_usize("max_l0_runs", self.max_l0_runs)?,
            max_mid_runs: nonzero_usize("max_mid_runs", self.max_mid_runs)?,
            max_decoded_input_rows_per_step: nonzero_usize(
                "max_decoded_input_rows_per_step",
                self.max_decoded_input_rows_per_step,
            )?,
        })
    }

    /// Rejects zero step budgets.
    pub fn validate(self) -> Result<(), GrepWorkerConfigError> {
        self.build_policy()?;
        Ok(())
    }
}

impl Default for GrepWorkerConfig {
    fn default() -> Self {
        let policy = GramIndexBuildPolicy::default();
        Self {
            max_files_per_step: policy.max_files_per_step.get(),
            max_content_bytes_per_step: policy.max_content_bytes_per_step.get(),
            max_rows_per_segment: policy.max_rows_per_segment.get(),
            max_l0_runs: policy.max_l0_runs.get(),
            max_mid_runs: policy.max_mid_runs.get(),
            max_decoded_input_rows_per_step: policy.max_decoded_input_rows_per_step.get(),
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
    fn defaults_match_the_build_policy() {
        let config = GrepWorkerConfig::default();

        assert_eq!(config.build_policy(), Ok(GramIndexBuildPolicy::default()));
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn zero_policy_fields_are_rejected() {
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
                "max_decoded_input_rows_per_step",
                GrepWorkerConfig {
                    max_decoded_input_rows_per_step: 0,
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
                "max_decoded_input_rows_per_step",
                GrepWorkerConfig {
                    max_decoded_input_rows_per_step: 0,
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
