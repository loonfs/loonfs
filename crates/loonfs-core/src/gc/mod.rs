//! Mark-and-sweep garbage collection (format spec, "Garbage collection").
//!
//! GC, floor advancement, and explicit namespace repair are the only code
//! paths that list the store. Nothing sweeps by default: callers opt in
//! through the admin endpoint or an explicit maintenance-step option.
//! Writers never coordinate with GC, so a sweep can race an in-flight
//! publish or checkpoint creation. The grace window, delete-time
//! re-verification, and retain-on-ambiguity defaults close those races. When
//! in doubt, this module retains.
//!
//! Readers do not coordinate with it either. A read pins a head and the
//! basis manifest under it and then keeps reading through that pair, so the
//! grace window has to run from the moment an object stopped being
//! referenced rather than from the moment it was written. The reference
//! anchor in `live_set.rs` is what dates that moment.

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
