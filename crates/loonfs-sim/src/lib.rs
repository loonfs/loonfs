//! Deterministic simulation primitives for LoonFS tests.
//!
//! Seeded randomness ([`rng`]), a virtual clock ([`clock`]), and a
//! fault-injecting object store ([`fault_store`]) that can lose put
//! responses, serve stale reads, reject compare-and-swaps as stale, hide
//! recent objects from listings, and corrupt bytes — plus trace and replay plumbing
//! ([`trace`], [`replay`]) to reproduce any failing seed exactly.

pub mod clock;
pub mod failure;
pub mod fault;
pub mod fault_store;
pub mod id;
pub mod model;
pub mod namespace_summary;
pub mod object_operation;
pub mod replay;
pub mod rng;
pub mod scenario;
pub mod trace;

pub use clock::{DeterministicClock, SimClock, SimDuration, SimInstant};
pub use failure::SimFailure;
pub use fault::{FaultSchedule, ObjectStoreFault, ScheduledFault};
pub use fault_store::FaultInjectingObjectStore;
pub use id::SimIdGenerator;
pub use model::{ModelComparison, ModelState};
pub use namespace_summary::{summarize_namespace_objects, SimNamespaceObjectSummary};
pub use object_operation::{ObjectOperation, ObjectOperationKind};
pub use replay::{ReplayError, ReplaySeed};
pub use rng::{DeterministicRng, SimRng, SimSeed};
pub use scenario::{SimConfig, SimScenario};
pub use trace::{RunId, SharedSimTrace, SimEventResult, SimTrace, SimTraceEvent};
