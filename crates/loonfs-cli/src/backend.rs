//! In-memory HTTP hosting and grep index steps for CLI profiles.

mod host;
mod operations;
mod step_budget;

pub(crate) use host::{client, run_then_drain, MaintenanceHost};
pub(crate) use step_budget::StepBudget;
