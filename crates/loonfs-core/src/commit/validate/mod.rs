//! Commit operation validation.
//!
//! [`session`] layers rows accepted earlier in the commit over the loaded
//! metadata, and [`checks`] holds the operation rules the planner applies as
//! it compiles each semantic operation.

mod checks;
mod error;
mod session;
#[cfg(test)]
mod tests;

pub use error::{CommitOperand, CommitValidationError};
pub(crate) use session::CommitValidationSession;
