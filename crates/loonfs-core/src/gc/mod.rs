//! Mark-and-sweep garbage collection (format spec, "Garbage collection").
//!
//! GC, floor advancement, and explicit namespace repair are the only code
//! paths that list the store. Nothing sweeps by default: callers opt in
//! through the admin endpoint or an explicit maintenance-tick option.
//! Writers never coordinate with GC, so a sweep can race an in-flight
//! publish or checkpoint creation. The grace window, delete-time
//! re-verification, and retain-on-ambiguity defaults close those races. When
//! in doubt, this module retains.

mod config;
mod fork_checkpoints;
mod live_set;
mod reap;
mod run;
#[cfg(test)]
mod tests;

pub use config::{GcConfig, GcReport};
pub(crate) use reap::{reap_abandoned_bootstrap, AbandonedBootstrapReap};
pub use run::gc_namespace;
