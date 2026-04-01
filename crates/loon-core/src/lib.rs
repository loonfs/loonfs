//! Canonical metadata rules, commit planning, path resolution, and invariants.
//!
//! This crate encodes the authoritative rules for how namespace state changes: precondition
//! validation, inode ID allocation, WAL serialization, checkpoint construction, and named
//! invariant checking. It does not perform I/O — storage operations are in `loon-objectstore`.

#![forbid(unsafe_code)]

pub mod checkpoint;
pub mod commit;
pub mod content;
pub mod invariants;
pub mod metadata;
pub mod namespace;
pub mod path;
pub mod progress;
pub mod wal;
