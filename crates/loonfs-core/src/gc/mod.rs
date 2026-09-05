//! Mark-and-sweep garbage collection (format spec, "Garbage collection").
//!
//! Collectors share one durable run per namespace. A fixed clock, complete
//! on-disk marking, and terminal pin lifecycles protect concurrent writers
//! and readers while small budgets resume across hosts. Collection runs only
//! when explicitly requested or scheduled.

mod budget;
mod compaction_staging;
mod config;
mod cursor;
mod fork_checkpoints;
mod mark;
mod mark_index;
mod mark_table;
mod reap;
mod references;
mod run;
mod sweep;
#[cfg(test)]
mod tests;
mod uploads;
mod validate;

pub use budget::PassBudget;
pub use config::GcConfig;
pub use cursor::{GcCursorKeyspace, NamespaceGcCursor};
pub use reap::{delete_if_aged, GraceAge};
pub use run::gc_namespace;
