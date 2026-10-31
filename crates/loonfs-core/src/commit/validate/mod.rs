//! Commit operation validation.
//!
//! [`session`] layers rows accepted earlier in the commit over the loaded
//! metadata. [`operations`] validates each operation the planner compiles,
//! through the state checks in [`checks`], and [`subtrees`] adds the
//! `advance_subtree` deltas that end the commit.

mod checks;
mod error;
mod operations;
mod session;
mod subtrees;
#[cfg(test)]
mod tests;

pub use error::{CommitOperand, CommitValidationError};
pub(crate) use session::CommitValidationSession;
