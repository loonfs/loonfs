//! Readable scenario fixture types, rendering helpers, and test infrastructure.
//!
//! Scenario fixtures are YAML files under `tests/scenarios/` that describe expected protocol
//! behavior in human-readable form. They are treated as product artifacts and reviewed alongside
//! specs and ADRs.

#![forbid(unsafe_code)]

pub mod client;
pub mod explore;
pub mod fixtures;
pub mod invariants;
pub mod minimize;
pub mod render;
pub mod replay;
pub mod scenario;
pub mod seed;
pub mod snapshots;
pub mod tempdir;
