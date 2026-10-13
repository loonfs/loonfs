//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod metrics_instrumented_object_store;
mod namespace_listing;
mod objectstore_conformance;
mod provider_env;
