//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

mod common;
mod driver;
mod grams_index;
mod grep_service_differential;
mod grep_worker;
mod root_format;
mod root_store;
mod standalone;

// Seeding helpers the standalone binary shares with its own tests.
#[path = "../../src/test_seeding.rs"]
mod test_seeding;
