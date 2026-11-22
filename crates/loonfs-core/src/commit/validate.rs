//! The steps that check and write a commit's rows.
//!
//! [`session`] layers the rows that the commit's earlier steps wrote over the
//! loaded metadata. [`operations`] defines each step that a planner calls,
//! through the state checks in [`checks`].

mod checks;
mod error;
mod operations;
mod session;

pub use error::{CommitOperand, CommitValidationError};
pub(crate) use operations::NewChild;
pub(crate) use session::CommitValidationSession;
