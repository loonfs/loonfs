//! Commit operation validation.
//!
//! [`checks`] holds the operation rules the planner applies as it compiles
//! each semantic operation, and [`session`] layers rows accepted earlier in the
//! commit over the loaded metadata.

mod checks;
mod error;
mod session;
#[cfg(test)]
mod tests;

pub(crate) use checks::{validate_operations, CommitNumbering};
pub use error::{CommitOperand, CommitValidationError};
pub(crate) use session::PublishValidationSession;
