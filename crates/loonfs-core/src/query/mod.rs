//! Derived-index query planning.
//!
//! The read-path executors live beside the other reads (`path::read`);
//! this module holds the pure planning that turns patterns into index
//! probes.

pub(crate) mod grep_plan;
