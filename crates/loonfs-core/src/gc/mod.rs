//! Stateless garbage collection from current manifests and pins.

mod collect;
mod families;
mod fork_pins;
mod live_set;
mod options;
mod reap;
mod reclaim;
mod sweep;
#[cfg(test)]
mod tests;
mod uploads;

pub use collect::gc_namespace;
pub use options::GcOptions;
pub use reap::{delete_if_aged, grace_age, GraceAge};
