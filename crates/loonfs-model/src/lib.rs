//! Executable reference model of LoonFS metadata semantics.
//!
//! The model applies committed WAL deltas to an in-memory metadata state with
//! no storage, caching, or recovery concerns. After each commit it checks the
//! directory binding rules, and at any applied sequence it returns which
//! inodes are visible, where each is bound, what a path resolves to, and what
//! a directory lists. Differential tests replay the same logical commits
//! through this model and through `loonfs-core` and require identical rows
//! and results, making this crate the readable statement of what the
//! metadata protocol means. Never share code with core: divergence detection
//! is this crate's entire value.
//!
//! Both sides use the vocabulary types of `loonfs-types`, and one rule in that
//! crate is shared: [`NameKey::for_display_name`](loonfs_types::NameKey::for_display_name)
//! folds a display name into the key that bindings compare, so the
//! differential suite can't detect a bug in that rule.

mod bindings;
mod error;
mod genesis;
pub mod metadata;
pub mod visibility;

pub use error::{ModelError, Result};
pub use genesis::bootstrap_metadata_state;
