//! Executable reference model of LoonFS metadata semantics.
//!
//! The model applies committed WAL deltas to an in-memory metadata state with
//! no storage, caching, or recovery concerns. Differential tests replay the
//! same logical commits through this model and through `loonfs-core` and
//! require identical outcomes, making this crate the readable statement of
//! what the metadata protocol means. Never share code with core: divergence
//! detection is this crate's entire value.

mod genesis;
pub mod metadata;

pub use genesis::bootstrap_metadata_state;
