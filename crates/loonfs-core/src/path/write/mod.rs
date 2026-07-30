//! Path-oriented writes: the mutation request language, the planner that
//! compiles it into one commit's operations, and the publish planning
//! session.

// Test-support mutation helpers: production mutations flow through the
// commit engine; unit tests drive the same pipeline through these wrappers.
#[cfg(test)]
pub(crate) mod content_write;
#[cfg(test)]
pub(crate) mod ops;

mod intent;
mod plan_create;
mod plan_delete;
mod plan_restore;
mod plan_transfer;
pub(crate) mod planner;
mod planning_helpers;
mod session;

pub use intent::{FilesystemOperation, MutationRequest};
pub(crate) use planner::mutation_fingerprint;
pub(crate) use session::PublishPlanningSession;
