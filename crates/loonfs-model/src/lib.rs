//! Executable reference model of LoonFS metadata semantics.
//!
//! The model applies committed WAL deltas to an in-memory metadata state with
//! no storage, caching, or recovery concerns. After each commit it checks the
//! directory binding rules, and at any applied sequence it answers which
//! inodes are visible, where each is bound, what a path resolves to, and what
//! a directory lists. Differential tests replay the same logical commits
//! through this model and through `loonfs-core` and require identical rows
//! and answers, making this crate the readable statement of what the
//! metadata protocol means. Never share code with core: divergence detection
//! is this crate's entire value.

mod bindings;
mod error;
mod genesis;
pub mod metadata;
pub mod visibility;

pub use error::{Error, Result};
pub use genesis::bootstrap_metadata_state;
