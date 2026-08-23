//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

mod common;
mod golden_formats;
mod grams_index;
mod grep_service_differential;
mod grep_worker;
mod maintenance;
mod root_format;
mod root_store;
mod test_seeding;
