//! Stateless garbage collection from current manifests and checkpoint records.

mod budget;
mod collect;
mod config;
mod families;
mod fork_checkpoints;
mod live_set;
mod reap;
mod sweep;
#[cfg(test)]
mod tests;
mod uploads;

pub use budget::PassBudget;
pub use collect::gc_namespace;
pub use config::GcConfig;
pub use reap::{delete_if_aged, GraceAge};
