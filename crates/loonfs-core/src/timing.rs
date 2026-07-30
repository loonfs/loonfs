//! Local monotonic elapsed-time boundary for self-enforced publish budgets.
//!
//! The trait and its process-clock implementation live in
//! `loonfs-objectstore` (`loonfs_objectstore::timing`), where provider retry
//! deadlines consume them; this module re-exports them so engine code has
//! one import path for them. Crate-internal: nothing outside `loonfs-core`
//! reads the clock through this seam.
//! Budgets bound a writer's own segment-write-to-CAS window so the GC grace
//! window is deterministically safe; they gate only the writer's next action
//! (abandon-and-rebuild). No validator compares timestamps, nothing on the
//! wire carries these readings, and commit validity never consults time.

pub(crate) use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
