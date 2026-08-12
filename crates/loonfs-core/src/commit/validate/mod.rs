//! Commit validation and plan building.
//!
//! Commit requests and publication batches use the same metadata-backed
//! validation path. [`checks`] holds the operation rules, while [`view`]
//! layers rows accepted earlier in the commit over the loaded metadata.

mod checks;
mod plan_build;
#[cfg(test)]
mod tests;
mod view;

pub(crate) use checks::{validate_ops, OpValidationCursor};
pub(crate) use plan_build::{validate_commit_for_publish, PublishCommitValidationContext};
pub(crate) use view::PublishValidationView;
