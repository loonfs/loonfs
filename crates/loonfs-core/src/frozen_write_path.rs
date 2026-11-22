//! A temporary frozen copy of the write path, kept only so that a refactor of
//! the write path can be compared against it. It's deleted before the branch
//! that changes the write path merges.
//!
//! The copy contains the path planners and their publish planning session
//! from `path::write`, the commit operations from `commit::ops`, and commit
//! operation validation from `commit::validate`, as they were before the
//! refactor. It shares the validation error and every type that the refactor
//! doesn't change with the live code, and it calls nothing in the live
//! `path::write` or `commit::validate` modules. [`prepare_and_compare`]
//! prepares every commit with both and panics when the results differ.

mod comparison;
mod ops;
mod validate;
mod write;

pub(crate) use comparison::prepare_and_compare;
pub(crate) use write::PublishPlanningSession;
