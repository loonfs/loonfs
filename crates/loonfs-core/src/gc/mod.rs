//! Mark-and-sweep garbage collection (format spec, "Garbage collection").
//!
//! Garbage collection does not coordinate with readers or writers. Grace
//! periods, delete-time reference checks, and retain-on-error behavior protect
//! concurrent publications and pinned reads. Collection only runs when
//! explicitly requested or scheduled.

mod budget;
mod compaction_staging;
mod config;
mod cursor;
mod fork_checkpoints;
mod live_set;
mod reap;
mod run;
#[cfg(test)]
mod tests;
mod uploads;

pub use budget::PassBudget;
pub use config::GcConfig;
pub use reap::{delete_if_aged, AgedSweep};
pub use run::gc_namespace;
