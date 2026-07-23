//! Path-oriented writes: mutation intents, the planner that turns them
//! into commit plans, and the publish planning session.

// Test-support mutation helpers: production mutations flow through the
// commit engine; unit tests drive the same pipeline through these wrappers.
#[cfg(test)]
pub(crate) mod content_write;
#[cfg(test)]
pub(crate) mod ops;

mod intent;
pub(crate) mod planner;
mod session;

pub use intent::PathMutationIntent;
pub(crate) use planner::path_intent_fingerprint;
pub(crate) use session::PublishPlanningSession;
