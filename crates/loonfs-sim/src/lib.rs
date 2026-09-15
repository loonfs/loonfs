//! Deterministic simulation tools for LoonFS tests.
//!
//! The crate provides seeded randomness, a virtual clock, object-store fault
//! injection, and trace recording for reproducing failed simulation
//! seeds.
//!
//! External simulation drivers also use this public API, so some items have
//! no callers in this repository.

pub mod clock;
pub mod fault;
pub mod fault_store;
pub mod object_operation;
pub mod rng;
pub mod scenario;
pub mod trace;

pub use clock::{DeterministicClock, SimClock, SimDuration, SimInstant};
pub use fault::{FaultSchedule, ObjectStoreFault, ScheduledFault};
pub use fault_store::FaultInjectingObjectStore;
pub use object_operation::{ObjectOperation, ObjectOperationKind};
pub use rng::{DeterministicRng, SimRng, SimSeed};
pub use scenario::SimConfig;
pub use trace::{RunId, SharedSimTrace, SimEventResult, SimTrace, SimTraceEvent};
