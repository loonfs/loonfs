//! Commit operation validation.
//!
//! [`checks`] holds the operation rules the planner applies as it compiles
//! each semantic operation, and [`view`] layers rows accepted earlier in the
//! commit over the loaded metadata.

mod checks;
mod error;
#[cfg(test)]
mod tests;
mod view;

pub(crate) use checks::{validate_ops, CommitNumbering};
pub use error::{CommitOperand, CommitValidationError};
pub(crate) use view::PublishValidationView;
