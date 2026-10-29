//! Store contract probe requests and results for the v0 HTTP API.

use crate::ProbeRunId;
use serde::{Deserialize, Serialize};

/// A request for one run of the store contract probe, with an empty body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StoreProbeRequest {}

/// The results of one store contract probe, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeResponse {
    /// The ID of this probe run. The server generates the ID and writes the probe's temporary
    /// objects under it.
    pub run_id: ProbeRunId,
    /// The check results, in the order that the checks ran.
    pub checks: Vec<StoreProbeCheckResult>,
}

impl StoreProbeResponse {
    /// Returns true when every check passed or reported an unsupported option.
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|check| {
            matches!(
                check.outcome,
                StoreProbeCheckOutcome::Passed | StoreProbeCheckOutcome::Unsupported
            )
        })
    }
}

/// The result of one contract check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeCheckResult {
    /// The check's stable name.
    pub name: String,
    /// Whether the store passed the check, failed it, or doesn't support the capability.
    pub outcome: StoreProbeCheckOutcome,
    /// The expected and actual behavior for a failed check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
}

impl StoreProbeCheckResult {
    /// Formats the check as one report line.
    pub fn check_line(&self) -> String {
        match self.outcome {
            StoreProbeCheckOutcome::Passed => format!("{}: passed", self.name),
            StoreProbeCheckOutcome::Unsupported => format!("{}: unsupported", self.name),
            StoreProbeCheckOutcome::Failed => match &self.message {
                Some(message) => format!("{}: failed: {message}", self.name),
                None => format!("{}: failed", self.name),
            },
        }
    }
}

/// What one contract check found about the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum StoreProbeCheckOutcome {
    /// The store behaved as the contract requires.
    Passed,
    /// The store doesn't support this optional capability.
    Unsupported,
    /// The store violated the contract or the operation failed.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_probe_requests_are_strict() {
        // The probe body carries no options yet, so an unknown one is the
        // only thing it can be sent.
        serde_json::from_value::<StoreProbeRequest>(serde_json::json!({}))
            .expect("an empty probe body decodes");
        serde_json::from_value::<StoreProbeRequest>(serde_json::json!({"deep": true}))
            .expect_err("an unknown probe field should be rejected");
    }
}
