//! Monotonic timing used to enforce local publication budgets.
//!
//! The timer implementation lives in `loonfs-objectstore` and is re-exported
//! here for a consistent internal import path. It measures the writer's
//! segment-write-to-CAS interval and determines when the writer must abandon
//! and rebuild an over-budget publication.
//!
//! These readings are never stored, compared by validators, or used to
//! determine commit validity.

pub(crate) use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
