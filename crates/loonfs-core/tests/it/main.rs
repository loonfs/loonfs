//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

mod attributes;
mod batch_publish;
mod change_feed;
mod commit_validation;
mod common;
mod differential;
mod fork_lifecycle;
mod layout_acceptance;
mod path_intents;
mod upload_sessions;
mod visibility_equivalence;
