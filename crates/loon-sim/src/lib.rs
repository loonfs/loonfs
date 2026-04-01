//! Deterministic simulator runtime for fault injection, actor stepping, delivery ordering,
//! restart events, and replay traces.
//!
//! Provides a controlled execution environment for testing concurrent and failure-sensitive
//! paths with reproducible seeds. Every test run with the same seed produces identical behavior.

#![forbid(unsafe_code)]

pub mod faults;
pub mod runtime;
pub mod trace;

pub use runtime::SimRuntime;
pub use trace::{SimActorId, SimDelivery, SimTraceEvent};
