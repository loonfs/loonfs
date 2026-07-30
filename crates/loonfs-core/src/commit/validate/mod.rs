//! Commit validation and plan building.
//!
//! One generic implementation serves both commit entry points:
//! [`build_commit_plan`] validates against the in-memory
//! [`InMemoryValidationView`] and [`build_commit_plan_for_publish`] against
//! the publish [`PublishValidationView`]. The split keeps every precondition
//! rule in exactly one place:
//!
//! - [`view`] defines the [`CommitValidationView`] lookup contract both
//!   metadata views implement.
//! - [`checks`] holds the single op-validation loop and its check helpers,
//!   parameterized by error constructor instead of cloned per error variant.
//!
//! [`InMemoryValidationView`]: view::InMemoryValidationView
//! [`PublishValidationView`]: view::PublishValidationView
//! [`CommitValidationView`]: view::CommitValidationView

mod checks;
mod plan_build;
#[cfg(test)]
mod tests;
mod view;

pub(crate) use checks::{validate_ops, OpValidationCursor};
#[cfg(test)]
pub(crate) use plan_build::build_commit_plan;
pub(crate) use plan_build::{
    allocates_inode, build_commit_plan_for_publish, PublishCommitValidationContext,
};
pub(crate) use view::PublishValidationView;
