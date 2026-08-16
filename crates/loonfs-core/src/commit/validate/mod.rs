//! Commit operation validation.
//!
//! [`checks`] holds the operation rules the planner applies as it compiles
//! each semantic operation, while [`view`] layers rows accepted earlier in
//! the commit over the loaded metadata.

mod checks;
#[cfg(test)]
mod tests;
mod view;

pub(crate) use checks::validate_ops;
pub(crate) use view::PublishValidationView;
