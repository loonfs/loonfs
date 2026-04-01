//! Sharded, object-storage-backed background-work coordination.
//!
//! Manages durable job queues for background work (snapshots, GC) that must survive process
//! restarts. Queue state is stored as JSON objects in the object store, using compare-and-swap
//! for consistency.

#![forbid(unsafe_code)]

pub mod broker;
pub mod durable;
pub mod repair;
pub mod types;
pub mod worker;
